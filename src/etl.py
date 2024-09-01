import os
import pandas as pd
from statsbombpy import sb
from dotenv import load_dotenv
import numpy as np
import psycopg2
from psycopg2.extras import execute_values
import warnings
from fuzzywuzzy import process

warnings.filterwarnings('ignore')   # Suppress the warning for only using free statsbomb data

load_dotenv()

def get_db_connection():
    """
    Connect to the AWS RDS database.
    """
    return psycopg2.connect(
        host=os.getenv('AWS_RDS_HOST'),
        port=os.getenv('AWS_RDS_PORT'),
        database=os.getenv('AWS_RDS_DATABASE'),
        user=os.getenv('AWS_RDS_USERNAME'),
        password=os.getenv('AWS_RDS_PASSWORD')
    )

def extract_data(matches, stadiums):
    """
    Extracts only the necessary events data for injury analysis
    """
    all_events = []
    for _, match in matches.iterrows():
        events = sb.events(match['match_id'])
        
        # Filter events to only include injuries
        injury_events = events[events['type'] == 'Substitution']
        injury_events = injury_events[injury_events['substitution_outcome'] == 'Injury']

        if not injury_events.empty:
            player_events = events[events['player'].isin(injury_events['player'])]
            player_events = player_events[player_events['minute'] <= injury_events['minute'].max()]
            all_events.append(player_events)

    return pd.concat(all_events, ignore_index=True) if all_events else pd.DataFrame(), pd.read_csv(stadiums)

def transform_data(events, stadiums, matches):
    """
    Transform the extracted data into the final format.
    """
    def get_best_fit(name, choice):
        """
        Fuzzy match the location of the stadium.
        """
        return process.extractOne(name, choice) #[0]
    
    def calculate_distance(events):
        """
        Calculate the distance covered for each player up to their injury.
        """
        distances = []
        for player in events['player'].unique():
            player_events = events[events['player'] == player].sort_values('minute')
            locations = player_events['location'].dropna().tolist()
            
            if len(locations) > 1:
                distances.append({
                    'player': player,
                    'distance_covered': np.sum(np.sqrt(np.sum(np.diff(locations, axis=0)**2, axis=1)))
                })
        
        return pd.DataFrame(distances)

    # Calculate distances
    distances_df = calculate_distance(events)
    
    # Get injury events
    injury_events = events[events['type'] == 'Substitution']
    injury_events = injury_events[injury_events['substitution_outcome'] == 'Injury']
    
    # Merge injury events with distances
    final_data = injury_events.merge(distances_df, on='player', how='left')
    
    # Select and rename columns
    columns = {
        'match_id': 'match_id',
        'player_id': 'player_id',
        'player': 'player',
        'substitution_outcome': 'substitution_outcome',
        'minute': 'minute',
        'distance_covered': 'distance_covered'
    }
    final_data = final_data[columns.keys()].rename(columns=columns)
    
    # Merge with match data
    match_columns = ['match_id', 'match_date', 'kick_off', 'home_team', 'stadium']
    final_data = final_data.merge(matches[match_columns], on='match_id')

    # Fuzzy match the location of the stadium on team name and stadium name
    matched_locations = []
    for _, row in final_data.iterrows():
        # Get best match and similarity score
        team_match, team_score = get_best_fit(row['home_team'], stadiums['team'])[:2]
        stadium_match, stadium_score = get_best_fit(row['stadium'], stadiums['stadium'])[:2]
        
        # Choose location based on most similar attribute (team or stadium)
        if team_score >= stadium_score:
            location = stadiums[stadiums['team'] == team_match]['location'].values[0]
        else:
            location = stadiums[stadiums['stadium'] == stadium_match]['location'].values[0]
        
        matched_locations.append(location)

    # Add the matched locations to final_data
    final_data['city'] = matched_locations

    # Collect and join weather data for the city
    key = os.getenv('VISUAL_CROSSING_API_KEY')
    base = 'https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/'
    params = f'?key={key}&contentType=csv&unitGroup=us&include=current'

    # make the necessary queries for each match
    final_weather_data = []
    for i, row in final_data.iterrows():
        datetime = row['match_date'] + 'T' + row['kick_off'].replace(' ', '')
        query = f'{base}{row["city"].replace(" ", "")}/{datetime}/{params}'
        weather_data = pd.read_csv(query)[['temp', 'dew', 'humidity', 'precip', 'conditions']]
        weather_data['city'] = row['city']
        final_weather_data.append(weather_data)

    weather_df = pd.concat(final_weather_data, ignore_index=True)
    final_data = final_data.merge(weather_df, on='city')
    final_data.insert(0, 'key', None)
    final_data['key'] = final_data['match_id'].astype(str) + '_' + final_data['player_id'].astype(str) + '_' + final_data['minute'].astype(str) + '_' + final_data['match_date']
    final_data['player_id'] = final_data['player_id'].astype(int)

    return final_data.drop_duplicates()

def load_data(data):
    """
    Load the transformed data into the AWS RDS database.
    """
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS injuries (
                key TEXT PRIMARY KEY,
                match_id INTEGER,
                player_id INTEGER,
                player TEXT,
                substitution_outcome TEXT,
                minute INTEGER,
                distance_covered REAL,
                match_date DATE,
                kick_off TIME,
                home_team TEXT,
                stadium TEXT,
                city TEXT,
                temp REAL,
                dew REAL,
                humidity REAL,
                precip REAL,
                conditions TEXT
            )
        """)
        cur.execute("TRUNCATE TABLE injuries")

        execute_values(cur, 
            "INSERT INTO injuries (key, match_id, player_id, player, substitution_outcome, minute, \
            distance_covered, match_date, kick_off, home_team, stadium, city, temp, dew, humidity, precip, conditions) VALUES %s",
            data.values.tolist())
        conn.commit()
        print("Data loaded successfully.")
    except Exception as e:
        conn.rollback()
        print(f"An error occurred: {e}")
    finally:
        cur.close()
        conn.close()

def run_etl(matches):
    """
    Run the ETL process for the injuries data. For __main__ use.
    """
    # Get the directory of the script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(script_dir, 'locations.csv')

    # Extract, transform, and load the data
    raw_data = extract_data(matches, stadiums=path)
    transformed_data = transform_data(events=raw_data[0], stadiums=raw_data[1], matches=matches)
    load_data(transformed_data)
    
    print("ETL process completed successfully.")

    return transformed_data

def query_database():
    """
    A test function to ensure the etl process was successful. 
    """
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT * FROM injuries LIMIT 10")
        rows = cur.fetchall()
        for row in rows:
            print(row)
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    print("Running ETL process for testing locally...")
    matches = sb.matches(competition_id=44, season_id=107)  # MLS 2023 season (set the scheduler to run based off of match schedule and statsbomb upload schedule)
    run_etl(matches)
    query_database()    # A double to verify the data was loaded correctly
