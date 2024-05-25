
from airflow import DAG
import pendulum
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from 

import sys
sys.path.append('/path/to/your/plugins')


def select_fruit():
    fru

with DAG (
    dag_id ="infrun_first", # airflow 표지에 보이는 대그 이름 => 파일 이름과 동일하게
    start_datae = pendulum.datetime(2021,01,01),
    catchup= False, # start_date 부터 돌린다 현재시점 까지 단, 과거 시점은 한번에 돌리게 된다. -> 순서대로 X
    params = {} # task들에서 공통적으로 쓰일 변수를 지정해준다.
    
) as dag:
    bash =PythonOperator(
        task_id = 'First_python_task',
        bash_command='echo 1'
    )