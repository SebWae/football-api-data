from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from mymodules import api_calls, utils

default_args = {
    'owner': 'sebastian',
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}

def call_with_yesterday(**kwargs):
    yesterday = utils.date_of_yesterday()
    api_calls.register_fixtures(yesterday)

with DAG(
    dag_id='get_daily_fixtures',
    default_args=default_args,
    schedule_interval='0 5 * * *',
    catchup=False,
) as dag:

    run_script = PythonOperator(
        task_id='run_script',
        python_callable=call_with_yesterday,
    )
