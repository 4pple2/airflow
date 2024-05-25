
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime


# DAG 정의
with DAG(
    dag_id="dag_bash_operator",  # UI에서 보여지는 첫번째 제목
    start_date=datetime(2023, 3, 1),  # 시작 날짜
    catchup=False,  # 시작 날짜 이전의 실행 무시
    tags=["example", "example2"],  # 태그 목록
) as dag:
    # BashOperator를 사용하여 두 개의 Bash 명령 실행
    bash_t1 = BashOperator(
        task_id="bash_t1",
        bash_command="echo whoami",
    )

    bash_t2 = BashOperator(
        task_id="bash_t2",
        bash_command="echo $HOSTNAME",
    )

    # bash_t1이 완료되면 bash_t2를 실행
    bash_t1 >> bash_t2

