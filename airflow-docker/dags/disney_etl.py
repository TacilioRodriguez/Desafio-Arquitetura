import requests
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.dates import days_ago
import pandas as pd
import json
from io import StringIO, BytesIO
from datetime import datetime


# Função para expandir os dados em formato JSON
def flatten_json(data, parent_key="", sep="."):
    items = []
    for key, value in data.items():
        new_key = parent_key + sep + key if parent_key else key
        if isinstance(value, dict):
            items.extend(flatten_json(value, new_key, sep=sep).items())
        elif isinstance(value, list):
            if len(value) > 0 and isinstance(value[0], dict):
                for i, sub_item in enumerate(value):
                    items.extend(flatten_json(sub_item, f"{new_key}[{i}]", sep=sep).items())
            else:
                items.append((new_key, value))
        else:
            items.append((new_key, value))
    return dict(items)

# Consultando API Disney para salvar na camada Bronze
def fetch_data_and_save_to_s3(**kwargs):
    bucket_name = 'lake-poseidon'
    output_prefix = 'bronze/'

    # Fazendo conexão com API
    api_response = requests.get('https://api.disneyapi.dev/character')
    
    # Verifica se a conexão ocorreu 
    if api_response.status_code != 200:
        print(f"Falha ao buscar dados da API Disney. Código de status: {api_response.status_code}")
        return

    # Verificar se tem dados retornando
    try:
        data = api_response.json()
    except json.decoder.JSONDecodeError as e:
        print(f"Falha ao decodificar a resposta JSON da API Disney: {e}")
        return

    # Verifica e valida se contem ca chave data no dicionario JSON
    if 'data' in data:
        data = data['data']
    else:
        print(f"Formato de dados inesperado da API Disney. Chave 'dados' não encontrada.")
        return

    # Verifica os dados para transformar em DataFrame
    flattened_data = [flatten_json(item) for item in data]

    # Transforma em dataframe
    df = pd.DataFrame(flattened_data)

    
    # Converte para csv 
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)

    # Salva na camada Bronze
    s3_hook = S3Hook(aws_conn_id='aws_s3')
    s3_hook.load_string(
        string_data=csv_buffer.getvalue(),
        key=f'{output_prefix}disney_data.csv',
        bucket_name=bucket_name,
        replace=True
    )
    print("Dados salvos no S3 - Bronze")

def move_bronze_to_silver(**kwargs):
    bucket_name = 'lake-poseidon'
    bronze_prefix = 'bronze/'
    silver_prefix = 'silver/'

    s3_hook = S3Hook(aws_conn_id='aws_s3')
    csv_data = s3_hook.read_key(
        key=f'{bronze_prefix}disney_data.csv',
        bucket_name=bucket_name,
    )
    # Verifica se a dados na camada Bronze
    if csv_data is None:
        print("Nenhum dado encontrado na camada Bronze.")
        return

    # Le os arquivos .csv
    df_bronze = pd.read_csv(StringIO(csv_data))

    # Transforma para Parquet
    parquet_buffer = BytesIO()
    df_bronze.to_parquet(parquet_buffer, index=False)
    parquet_buffer.seek(0)

    s3_hook.load_file_obj(
        file_obj=parquet_buffer,
        key=f'{silver_prefix}disney_data.parquet',
        bucket_name=bucket_name,
        replace=True
    )
    print("Dados movidos para S3 - Silver")

# Função para limpar os dados e retirar caracteres antes de salvar na camada Gold
def clean_column(value):
    if value in ('[]', "''", '""'):
        return ''
    return value.strip('\'"[]')

def move_silver_to_gold(**kwargs):
    bucket_name = 'lake-poseidon'
    silver_prefix = 'silver/'
    gold_prefix = 'gold/'

    logical_date = kwargs['logical_date']
    formatted_date = logical_date.strftime('%d-%m-%Y')

    s3_hook = S3Hook(aws_conn_id='aws_s3')
    silver_files = s3_hook.list_keys(bucket_name=bucket_name, prefix=silver_prefix)

    if not silver_files:
        print("Nenhum arquivo encontrado na camada Silver.")
        return

    for file in silver_files:
        key_obj = s3_hook.get_key(
            key=file,
            bucket_name=bucket_name,
        )

        if not key_obj:
            print(f"Falha ao ler o arquivo {file} da camada Silver.")
            continue

        silver_data = BytesIO(key_obj.get()["Body"].read())

        if silver_data.getbuffer().nbytes == 0:
            print(f"File {file} is empty.")
            continue

        try:
            df_silver = pd.read_parquet(silver_data)
        except Exception as e:
            print(f"Falha ao ler o arquivo Parquet {file}: {e}")
            continue

        # Limpando dataframe e incluindo colunas
        columns_to_clean = ['films', 'shortFilms', 'tvShows', 'videoGames', 'parkAttractions', 'allies', 'enemies']
        for column in columns_to_clean:
            df_silver[column] = df_silver[column].astype(str).apply(clean_column)

        # Salvando DataFrame tratado em formato parquet
        buffer = BytesIO()
        df_silver.to_parquet(buffer, index=False)
        buffer.seek(0)

        gold_key = f'{gold_prefix}disney_{formatted_date}.parquet'
        s3_hook.load_bytes(
            bytes_data=buffer.read(),
            key=gold_key,
            bucket_name=bucket_name,
            replace=True
        )

    print("Dados movidos para S3 - Gold")

with DAG(
    'disney_etl',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
    },
    description='Pipeline ETL para processar dados da API Disney e armazená-los em camadas S3',
    schedule_interval='*/5 * * * *',
    start_date=days_ago(1),
    catchup=False,
) as dag:

    fetch_data_task = PythonOperator(
        task_id='fetch_data_task',
        python_callable=fetch_data_and_save_to_s3,
        provide_context=True,
    )

    move_bronze_to_silver_task = PythonOperator(
        task_id='move_bronze_to_silver_task',
        python_callable=move_bronze_to_silver,
        provide_context=True,
    )

    move_silver_to_gold_task = PythonOperator(
        task_id='move_silver_to_gold_task',
        python_callable=move_silver_to_gold,
        provide_context=True,
    )

    fetch_data_task >> move_bronze_to_silver_task >> move_silver_to_gold_task
