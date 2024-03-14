from airflow import DAG

from datetime import datetime, timedelta
from airflow.hara.hara_operator.cwl_command_line_tool_operator import CwlCommandLineToolOperator
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'harada',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='workdir_dockercwl_local_dag',
    default_args=default_args,
    description="test for workdir",
    start_date=datetime(2024, 1, 1, 0),
    schedule_interval='@daily'
) as tag:
    task0 = BashOperator(
        task_id='task_0',
        bash_command='echo "hara Start"'
    )

    task1 = CwlCommandLineToolOperator(
        task_id='task_1',  # cwltool echo.cwl.yaml --message_text="hello typing"
        cwl_path='/home/typingliu/workspace/tpy/airflow21/airflow/airflow/hara_bin/hara_dags/workdir_dockercwl_local_dag/hara_docker_hello.cwl.yaml',
        job_path='/home/typingliu/workspace/tpy/airflow21/airflow/airflow/hara_bin/hara_dags/workdir_dockercwl_local_dag/docker-job.yaml'
    )
    task2 = BashOperator(
        task_id='task_2',
        bash_command='echo "Hara, Congratulations!"'
    )

    task0 >> task1 >> task2
