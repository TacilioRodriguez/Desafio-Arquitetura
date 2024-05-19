from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'consulta_api',
    default_args=default_args,
    description='DAG para logar na API da Disney',
    schedule_interval=timedelta(minutes=5),
    catchup=False
) as dag:

    get_disney_data = SimpleHttpOperator(
        task_id='get_disney_data',
        http_conn_id='disney_api',
        endpoint='character',
        method='GET',
        log_response=True
    )

    get_disney_data