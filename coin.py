from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
#from airflow.providers.common.sql.operators.sql import SQLExecuteOperator 
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

import requests
import logging
import json
import os

default_args ={
    'owner' : 'kim'
}

dag_args = dict(
    dag_id="call_snowflake",
    default_args= default_args,
    description='call snowflake from airflow',
    schedule_interval=timedelta(minutes=50),
    start_date=datetime(2022, 2, 1),
    catchup=False,
    tags=['test'],
)

def extract_price_data():
    data_list = []
    i = 0
    for i in range(0,1000):

        _url = f'https://api.odcloud.kr/api/15001743/v1/uddi:f5124071-9bc1-48bf-8b16-ab43815eba69?serviceKey=eEvY5rw3KhX0Q%2Fau%2FJznLrul9agKg%2Br4bhNMVpwj%2BJeUkofgcyNTQIZkzdSQBmkbMcnsaPUTAEsH5xTZUYtmXA%3D%3D&pageNo={i}&numOfRows=10&type=json'

        response = requests.get(_url)

    # 응답 코드 확인
        if 200 <= response.status_code <= 300:
            _content = response.content.decode('utf-8')
        else:
            _content = response.content.decode('utf-8')

        _public_data = json.loads(_content)
        
        for i in _public_data['data']:
            data_list.append(i)
    
    return data_list

def Upload_S3_LocalData(**context):
    _raw_data = context['ti'].xcom_pull(
        task_ids = 'extract_price_data', key="return_value"  # task_id를 가져올 task_id값을 적어줘야함
        )
    
    _file_path ='/opt/airflow/data/house_price.json'         # airflow에 마운트된 경로에 지정해줘야한다.

    with open(_file_path,'w',encoding='utf-8') as f:
        json.dump(_raw_data, f, ensure_ascii=False, indent=4)

    return 0

def Upload_S3_Direct(**context):
    _raw_data = context['ti'].xcom_pull(
        task_ids = 'extract_price_data', key="return_value"  # task_id를 가져올 task_id값을 적어줘야함
        )
    json_data = json.dumps(_raw_data, ensure_ascii=False)

    s3_hook = S3Hook(aws_conn_id ='AWS_conn_kjs')

    s3_hook.load_string(json_data, key = 's3://airflow2snow/airflow/house_price.json', bucket_name = 'airflow2snow', replace=True)





with DAG(dag_id='call_public_data', schedule_interval='@once', start_date=days_ago(n=1)) as dag:
    extract_price_data = PythonOperator(
        task_id = 'extract_price_data',
        python_callable= extract_price_data
    )

    change_json = PythonOperator(
        task_id = 'change_price',
        python_callable = Upload_S3_LocalData,
        op_kwargs = {'param1':'value'}
    )
    create_local_to_s3 = LocalFilesystemToS3Operator(
        task_id = 'Upload_S3',
        filename='/opt/airflow/data/house_price.json',
        dest_key = 's3://airflow2snow/s3://airflow2snow/airflow/house_price.json',
       # dest_bucket='airflow2snow'
        aws_conn_id ='AWS_conn_kjs'
    )
    Upload_S3_Direct = PythonOperator(
        task_id = 'upload_to_s3_task',
        python_callable=Upload_S3_Direct
    )
    

     
extract_price_data >> change_json >> create_local_to_s3 >> Upload_S3_Direct


