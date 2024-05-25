from datetime import datetime, timedelta
from airflow import DAG

from airflow.operators.empty import EmptyOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
snowflake_conn = SnowflakeOperator(snowflake_conn_id="snowflake_conn")

dag_args = dict(
    dag_id="call_snowflake",
    default_args= default_args,
    description='call snowflake from airflow',
    schedule_interval=timedelta(minutes=50),
    start_date=datetime(2022, 2, 1),
    catchup=False,
    tags=['test'],
)


with DAG( **dag_args) as dag:
    start = EmptyOperator(
        task_id='start',
        dag=dag
    )

    end = EmptyOperator(
        task_id='end',
        dag=dag
    )


    # Snowflake Operator Example
    snowflake_task_call = SnowflakeOperator(
        task_id="call_snowflake",
        snowflake_conn_id="snowflake_conn",
        sql="CALL PRCS.SP_DW_COMN_CD();",
        autocommit=True
    )


start >> snowflake_task_call >> end