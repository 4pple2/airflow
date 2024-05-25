from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# Python 함수 정의: context를 사용하여 DAG 정보 출력
 
def upstream(**context):
    context['ti'].xcom_push(key='my_test',value=23)
    return 19

def downstream(param1,**context):
    defualt_value= context['ti'].xcom_pull(
        task_ids = 'upstream_id', key="return_value"
        )
    give_value=context['ti'].xcom_pull(
        task_ids = 'upstream_id',  key='my_test'
        )

    print(f"defualt_value :  {defualt_value}")
    print(f"defualt_value : {give_value}")

with DAG(dag_id = 'xcom_test',schedule_interval='@once',start_date=datetime(2022,1,1))as dag:

    # BashOperator를 사용하여 두 개의 Bash 명령 실행
    upstream = PythonOperator(
        task_id = 'upstream_id',
        python_callable = upstream
    )
    downstream = PythonOperator(
        task_id = 'downstream_id',
        python_callable = downstream,
        op_kwargs = {'param1':'value'}
    )

upstream >> downstream

