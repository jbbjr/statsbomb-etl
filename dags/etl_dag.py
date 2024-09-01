from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from src.etl import run_etl
from statsbombpy import sb

def fetch_and_run_etl():
    """
    Fetch the matches from Statsbomb and trigger the ETL process
    """
    matches = sb.matches(competition_id=44, season_id=107)  # MLS 2023 season (set the scheduler to run based off of match schedule and statsbomb upload schedule)
    return run_etl(matches)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'statsbomb_etl',
    default_args=default_args,
    description='A simple ETL DAG for Statsbomb data',
    schedule_interval=timedelta(days=1),
)

run_etl_task = PythonOperator(
    task_id='run_etl',
    python_callable=fetch_and_run_etl,
    dag=dag,
)

run_etl_task