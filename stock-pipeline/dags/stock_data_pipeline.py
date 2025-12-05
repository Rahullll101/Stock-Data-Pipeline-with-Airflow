from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
sys.path.append('/opt/airflow/scripts')
from fetch_stock_data import fetch_and_store_stock_data

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'stock_data_pipeline',
    default_args=default_args,
    description='Fetch stock market data and store in PostgreSQL',
    schedule_interval='@daily',  # Change to '@hourly' for hourly updates
    catchup=False,
    tags=['stock', 'market-data'],
)

fetch_task = PythonOperator(
    task_id='fetch_and_store_stock_data',
    python_callable=fetch_and_store_stock_data,
    dag=dag,
)

fetch_task