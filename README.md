![SBbanner](images/statsbomb.png)

# StatsBomb to AWS RDS ETL Pipeline for Injury Prediction

## Project Overview

This project implements an automated ETL (Extract, Transform, Load) pipeline using Apache Airflow to process soccer match data from StatsBomb, focusing on preparing data for an injury prediction model. The pipeline extracts freely available MLS match data from the StatsBomb API, enhances it with weather data from the Visual Crossing API, and stores it in an AWS RDS PostgreSQL database.

## Key Components

1. **Dockerfile & docker-compose.yml**: Set up the Airflow environment with necessary dependencies.
2. **etl_dag.py**: Defines the Airflow DAG that schedules and executes the ETL process daily.
3. **etl.py**: Contains the core ETL logic, divided into extract, transform, and load functions.
4. **scrape_loc.py**: Scrapes stadium location data to relate match data to weather data.
5. **test_connections.py**: Ensures proper connectivity with all data sources and targets.

## ETL Process in Detail

### 1. Extraction (`extract_data` function)
- Fetches match data for the MLS 2023 season from StatsBomb API.
- Retrieves all events for each match and filters for substitution events marked as injuries.
- Collects all events for injured players up to the point of their injury.
- Reads stadium information from a CSV file.

### 2. Transformation (`transform_data` function)
- Calculates the distance covered by each player up to the point of injury.
- Merges injury events with calculated distances.
- Enriches data with match details (date, kick-off time, home team, stadium).
- Uses fuzzy matching to associate each stadium with its location (city).
- Fetches weather data for each match using the Visual Crossing API.
- Creates a unique key for each injury event.

### 3. Loading (`load_data` function)
- Establishes a connection to the AWS RDS PostgreSQL database.
- Creates a table named 'injuries' if it doesn't exist.
- Truncates the table to ensure fresh data.
- Inserts the transformed data into the 'injuries' table.

## Docker Configuration

This project uses Docker to ensure a consistent environment across different systems.

### Dockerfile
- Uses the official Apache Airflow 2.10.0 image as a base.
- Installs PostgreSQL client and build essentials.
- Copies and installs Python dependencies from requirements.txt.
- Copies the project files into the container.

### Service configurations (`docker-compose.yml`)
- postgres: PostgreSQL database for Airflow metadata.
- init: Initializes Airflow database and creates an admin user.
- webserver: Runs the Airflow web server.
- scheduler: Runs the Airflow scheduler.
- init-cleanup: Stops the init service after other services are up.

### Dependencies
The startup process is carefully orchestrated to ensure all services are properly initialized:

- The postgres service starts first, with a health check to ensure it's ready.
- The init service depends on postgres being healthy. It runs `init.sh` which initializes the airflow db and creates an admin user.
- The webserver and scheduler services depend on the init service being healthy.
- The init-cleanup service waits for webserver and scheduler to be up before stopping the init service.

## Data Features for Injury Prediction

The resulting dataset includes:

1. Match identifiers and basic info (ID, date, kick-off time)
2. Player identifiers and names
3. Injury occurrence (substitution due to injury)
4. Match duration until injury (in minutes)
5. Distance covered by the player before injury
6. Team and stadium information
7. Weather conditions (temperature, humidity, precipitation, general conditions)

## Setup and Usage

1. Clone the repository and navigate to the project directory.
2. Set up the `.env` file with necessary credentials (AWS RDS, Visual Crossing API).
3. Build and start the Docker containers:
4. Access Airflow web interface at `http://localhost:8080` (credentials: Admin/admin).
5. The DAG 'statsbomb_etl' will run daily, processing the latest available data.


## Testing

Run the following command to ensure all connections are working properly:
```python
pytest tests/test_connections.py
```

## Notes

The dataset provides a good set of features for trying to predict injury. The initial idea was if the type of injury were to be specified, a model using weather and distance covered could maybe predict cramping prior to happening, enabling medical staff to fuel their athletes more effectively during matches. Class imbalance would be something to consider given injury events are rare, and time-series data might be interesting to include as injuries are a result of build-up typically, especially in the context of things like cramping. Consider changing the matches scraped to your team or league, and set the scheduler to a more appropriate cadence given your use case.