import warnings
warnings.filterwarnings(action = "ignore")

from airflow.operators.empty  import EmptyOperator
from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
import boto3

from airflow.operators.empty import EmptyOperator
# snowflake hook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

# aws hook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
# fcontrib.hooks.aws_hook import AwsHook을 대체
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
# dms 훅
from airflow.providers.amazon.aws.operators.dms import (
    DmsCreateTaskOperator,
    DmsDeleteTaskOperator,
    DmsStartTaskOperator,
)
# dms 센서
from airflow.providers.amazon.aws.sensors.dms import DmsTaskCompletedSensor

import json

AWS_DEFAULT_REGION = "ap-northeast-2"

CREATE_ASSETS = {
    'source_endpoint_arn' : 'arn:aws:dms:ap-northeast-2:992382800329:endpoint:ENSPVEDUDVSGQULVVGQCJFYUKTVZOAG3HQVSU6Y',
    'target_endpoint_arn' : 'arn:aws:dms:ap-northeast-2:992382800329:endpoint:AR7VZYCIZZJSLT4NUV3WIPRG3RQBXNTP7C2VN5Q',
    'replication_instance_arn' : 'arn:aws:dms:ap-northeast-2:992382800329:rep:WJW4UILSNC2IJSUT7NXLKPDHSDUMWLRAFAHR3SA',
    # ㄴ dms 복제 인스턴스 arm
    'migration_type': 'full-load-and-cdc',      
    'aws_conn_id' : 'AWS_conn' 
    # ㄴ Arflow에 생성되어 있는 connection 이름
}

FILTER_DATE = '2000-02-19'
DMS_REPLICATION_TASK_NAME = 'airflow-test-kjs'
  

AWS_HOOK = AwsBaseHook(CREATE_ASSETS['aws_conn_id'])
CREDENTIALS = AWS_HOOK.get_credentials()

DMS_CLIENT = boto3.client("dms",
                          aws_access_key_id = CREDENTIALS.access_key,
                          aws_secret_access_key = CREDENTIALS.secret_key,
                          region_name = AWS_DEFAULT_REGION
)

TABLE_MAPPINGS = {
    "rules": [
        {
            "rule-type": "selection",
            "rule-id": "1",
            "rule-name": "1",
            "object-locator": {
                "schema-name": "ADMIN",
                "table-name": "CHLEE_TEST"
            },
            "rule-action": "include",
            "filters": []
        }
    ]
}


def Create_DMS_Task(**context):
    response = DMS_CLIENT.create_replication_task(
        ReplicationTaskIdentifier=DMS_REPLICATION_TASK_NAME,
        SourceEndpointArn = CREATE_ASSETS['source_endpoint_arn'],
        TargetEndpointArn = CREATE_ASSETS['target_endpoint_arn'],
        ReplicationInstanceArn= CREATE_ASSETS['replication_instance_arn'],
        MigrationType = CREATE_ASSETS['migration_type'],
        TableMappings = json.dumps(TABLE_MAPPINGS),
    )

    # create_replication_task() 함수로 태스크 생성 후, 생성이 완료될 때 까지 대기하는 함수이다.
    _waiter = DMS_CLIENT.get_waiter('replication_task_ready')
    _waiter.wait(Filters=[
        {
            'Name': "replication-task-arn",
            'Values': [response['ReplicationTask']['ReplicationTaskArn']]
        },
    ])
    context['ti'].xcom_push(key='xcom_push_value', value= response['ReplicationTask']['ReplicationTaskArn'])


task_arn = '{{ti.xcom_pull(key="xcom_push_value")}}'

default_args={
    'owner' :'airflow',
    'start_date' : datetime(2023,1,1),
    'retry_delay' : timedelta(minutes=5)
}

with DAG(
    dag_id= 'dms_test_dag',
    default_args= default_args,
    schedule_interval= '@once',
    catchup=False
)as dags:
    _task_start = EmptyOperator(
        task_id = "start_task"
    )
    
    create_task = PythonOperator(
        task_id='create_dms_task',
        provide_context=True,
        python_callable=Create_DMS_Task,
    )

    task_arn = '{{ti.xcom_pull(key="xcom_push_value")}}'

    start_task = DmsStartTaskOperator(
        task_id = "start_dms_task",
        replication_task_arn=task_arn,
        aws_conn_id = CREATE_ASSETS['aws_conn_id']
    )
    await_task_start = DmsTaskCompletedSensor(
        task_id="await_task_start",
        replication_task_arn=task_arn,
        aws_conn_id = CREATE_ASSETS['aws_conn_id'],
    )

    delete_task = DmsDeleteTaskOperator(
        task_id="delete_task",
        replication_task_arn=task_arn,
        aws_conn_id = CREATE_ASSETS['aws_conn_id'],
    )
    finish = EmptyOperator(
        task_id = "finish_task"
    )

_task_start >> create_task >> start_task >> await_task_start >> finish