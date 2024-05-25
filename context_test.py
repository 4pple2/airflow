from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.operators.python import PythonOperator

dag_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


def print_var(execution_date, logical_date, dag_run, ti):
    print(f"execution_date: {execution_date}")
    print(f"logical_date: {logical_date}")
    print(f"dag_id: {dag_run.dag_id}")
    print(f"dag_id: {ti.task_id}")


def print_context(**context):
    print(context)


def print_dag_id():
    context = get_current_context()
    print(context['dag_run'].dag_id)


@dag(start_date=datetime(2022, 1, 20),
     dag_id='print_context_var',
     default_args=dag_args,
     schedule="@once")

def generate_dag():
    t1 = PythonOperator(
        task_id="print_some_var",
        python_callable=print_var
    )
    t2 = PythonOperator(
        task_id="print_context",
        python_callable=print_context
    )


generate_dag()