from datetime import datetime
from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python_operator import PythonOperator
import pandas as pd
import os
import json
import pyarrow.parquet as pq

# Define default_args
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

# Define a DAG
dag = DAG(
    'silver_s3',
    default_args=default_args,
    description='A DAG to process parquet files from S3 and store as tables',
    schedule_interval='@daily',
)

# Path configuration
bucket_name = 'lake-poseidon'
input_prefix = 'bronze/'
output_prefix = 'silver/'

# Python function to check for parquet files and process them
def process_parquet_files(**kwargs):
    s3_hook = S3Hook(aws_conn_id='aws_s3')
    keys = s3_hook.list_keys(bucket_name, prefix=input_prefix)
    
    parquet_files = [key for key in keys if key.endswith('.parquet')]
    
    if not parquet_files:
        print("No .parquet files found.")
        return
    
    for file_key in parquet_files:
        file_obj = s3_hook.get_key(file_key, bucket_name)
        file_path = f'/tmp/{file_key.split("/")[-1]}'
        
        # Download parquet file to local
        file_obj.download_file(file_path)
        
        # Read parquet file
        pq_file = pq.ParquetFile(file_path)
        table = pq_file.read()
        df = table.to_pandas()
        
        # Save processed data as a new parquet file
        processed_file_path = f'/tmp/processed_{file_key.split("/")[-1]}'
        df.to_parquet(processed_file_path, index=False)
        
        # Upload the processed parquet file to S3
        s3_hook.load_file(
            filename=processed_file_path,
            key=f'{output_prefix}{file_key.split("/")[-1]}',
            bucket_name=bucket_name,
            replace=True
        )
        
        # Clean up local files
        os.remove(file_path)
        os.remove(processed_file_path)

# PythonOperator for processing parquet files
process_files_task = PythonOperator(
    task_id='process_parquet_files',
    python_callable=process_parquet_files,
    provide_context=True,
    dag=dag,
)

process_files_task
