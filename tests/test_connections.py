import pytest
import psycopg2
from airflow.models import DagBag
from statsbombpy import sb
import os
import requests
from dotenv import load_dotenv

load_dotenv()

# Test Statsbomb connection
def test_statsbomb_connection():
    competitions = sb.competitions()
    assert len(competitions) > 0, "Failed to fetch competitions from Statsbomb"

# Test RDS connection
def test_rds_connection():
    conn = psycopg2.connect(
        host=os.getenv('AWS_RDS_HOST'),
        port=os.getenv('AWS_RDS_PORT'),
        database=os.getenv('AWS_RDS_DATABASE'),
        user=os.getenv('AWS_RDS_USERNAME'),
        password=os.getenv('AWS_RDS_PASSWORD')
    )
    cur = conn.cursor()
    cur.execute("SELECT 1")
    result = cur.fetchone()
    cur.close()
    conn.close()
    assert result[0] == 1, "Failed to connect to RDS"

# Test Airflow DAG loading
def test_dags_load_with_no_errors():
    dag_bag = DagBag(dag_folder='dags', include_examples=False)
    assert len(dag_bag.import_errors) == 0, f"DAG import errors: {dag_bag.import_errors}"

# Test Airflow database connection
def test_airflow_db_connection():
    from airflow.utils.db import create_session
    with create_session() as session:
        result = session.execute("SELECT 1").scalar()
    assert result == 1, "Failed to connect to Airflow database"

# Test Visual Crossing API connection
def test_visual_crossing_connection():
    key = os.getenv('VISUAL_CROSSING_API_KEY')
    base = 'https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/'
    params = f'?key={key}&contentType=csv&unitGroup=us&include=current'

    query = f'{base}33.753746,-84.386330{params}'
    assert requests.get(query).status_code == 200, "Failed to connect to Visual Crossing API"

if __name__ == "__main__":
    pytest.main([__file__])