from airflow import DAG

from datetime import datetime, timedelta
from airflow.hara.hara_operator.cwl_local import CwlLocalOperator
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'harada',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='hara_docker_cwl_dag_0',
    default_args=default_args,
    description="hara's first docker cwl dag",
    start_date=datetime(2024, 1, 1, 0),
    schedule_interval='@daily'
) as tag:
    task0 = BashOperator(
        task_id='task_0',
        bash_command='echo "start"'
    )

    task1 = CwlLocalOperator(
        task_id='task_1',
        cwl_path='/home/typingliu/workspace/tpy/airflow21/airflow/airflow/hara_bin/hara_dags/first_docker_cwl_dag/hara_docker_hello.cwl.yaml',
        job_path='/home/typingliu/workspace/tpy/airflow21/airflow/airflow/hara_bin/hara_dags/first_docker_cwl_dag/docker-job.yaml'
    )
    task2 = BashOperator(
        task_id='task_2',
        bash_command='echo "Good job"'
    )

    task0 >> task1 >> task2
