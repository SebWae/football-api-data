from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import subprocess

default_args = {
    'owner': 'sebastian',
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}

def run_my_script():
    subprocess.run(['python3', '/home/airflow/gcs/data/my_script.py'])

with DAG(
    dag_id='daily_python_script_test_summer',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:

    run_script = PythonOperator(
        task_id='run_script',
        python_callable=run_my_script
    )
