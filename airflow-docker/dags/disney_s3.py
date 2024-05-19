from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.utils.dates import days_ago
import pandas as pd
import json

def process_and_save_to_s3(**kwargs):
    # Obter os dados da resposta da API
    response_data = kwargs['ti'].xcom_pull(task_ids='get_disney_data')

    # Parsear os dados da API
    data = json.loads(response_data)
    
    # Extrair a lista de personagens
    characters = data.get('data')
    
    # Verificar se os dados dos personagens não estão vazios
    if not characters:
        raise ValueError("Nenhum dado foi retornado da API")
    
    # Organizar os dados em um DataFrame
    df = pd.DataFrame([characters])
    
    # Salvar o DataFrame no formato Parquet
    parquet_filename = f"/tmp/disney_data_{kwargs['execution_date'].strftime('%Y%m%dT%H%M%S')}.parquet"
    df.to_parquet(parquet_filename, index=False)
    
    # Carregar o arquivo Parquet no S3
    s3_hook = S3Hook(aws_conn_id='aws_s3')
    s3_key = f"bronze/disney_data_{kwargs['execution_date'].strftime('%Y%m%dT%H%M%S')}.parquet"
    s3_hook.load_file(parquet_filename, s3_key, bucket_name='lake-poseidon', replace=True)

with DAG(
    'disney_s3',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
    },
    description='Uma DAG para consultar a API da Disney e salvar os dados no S3 em formato Parquet',
    schedule_interval='*/5 * * * *',
    start_date=days_ago(1),
    catchup=False,
) as dag:

    get_disney_data = SimpleHttpOperator(
        task_id='get_disney_data',
        http_conn_id='disney_api',
        endpoint='character',
        method='GET',
        response_filter=lambda response: response.text,
        log_response=True,
    )

    process_and_save_to_s3 = PythonOperator(
        task_id='process_and_save_to_s3',
        python_callable=process_and_save_to_s3,
        provide_context=True,
    )

    get_disney_data >> process_and_save_to_s3
