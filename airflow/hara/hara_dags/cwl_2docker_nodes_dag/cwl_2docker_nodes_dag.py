from airflow import DAG

from datetime import datetime, timedelta
from airflow.hara.hara_operator.cwl_local_pyoperator import CwlLocalOperator
from airflow.operators.bash import BashOperator
import os

default_args = {
    'owner': 'harada',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

main_cwl_file_path = "/home/typingliu/workspace/tpy/airflow25/airflow/airflow/hara/hara_dags/cwl_2docker_nodes_dag/main.cwl.yaml"
job_file_path = "/home/typingliu/workspace/tpy/airflow25/airflow/airflow/hara/hara_dags/cwl_2docker_nodes_dag/hara_job.yaml"
basedir = '/home/typingliu/workspace/tpy/airflow25/airflow/airflow/hara/hara_dags/cwl_2docker_nodes_dag'
cwl_work_path = '/home/typingliu/temp/'

with DAG(
    dag_id='first_controled_docker_cwl_dag',
    default_args=default_args,
    description="hara's first local controlled docker cwl dag",
    start_date=datetime(2024, 1, 1, 0),
    schedule_interval='@daily'
) as tag:
    task0 = BashOperator(
        task_id='task_0',
        bash_command='echo "hara Start"'
    )

    task1 = CwlLocalOperator(
        task_id='task_1',  # cwltool echo.cwl.yaml --message_text="hello typing"
        main_cwl_file_path=main_cwl_file_path,
        cwl_step_to_run='writeMessage',
        basedir=basedir,
        job_file_path=job_file_path,
        cwl_work_path=main_cwl_file_path,
    )

    task2 = CwlLocalOperator(
        task_id='task_2',  # cwltool echo.cwl.yaml --message_text="hello typing"
        main_cwl_file_path=main_cwl_file_path,
        cwl_step_to_run='countWords',
        basedir=basedir,
        job_file_path=job_file_path,
        cwl_work_path=cwl_work_path,
    )

    task3 = BashOperator(
        task_id='task_3',
        bash_command='echo "Hara, Congratulations!"'
    )

    task0 >> task1 >> task2 >> task3
