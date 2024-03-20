from airflow import DAG

from datetime import datetime, timedelta
from airflow.hara.hara_operator.cwl_local_pyoperator import CwlLocalOperator
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'harada',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

main_cwl_file_path = '/home/typingliu/workspace/tpy/airflow25/airflow/airflow/hara/cwl_tools/rw_example/hara_workflow.cwl.yaml'
job_file_path = '/home/typingliu/workspace/tpy/airflow25/airflow/airflow/hara/cwl_tools/rw_example/hara_job.yaml'
basedir = '/home/typingliu/workspace/tpy/airflow25/airflow/airflow/hara/cwl_tools/rw_example'  # cwl file path
cwl_work_path = '/home/typingliu/temp/'


with DAG(
    dag_id='cwl_1node_dag',
    default_args=default_args,
    description="hara's first local cwl dag",
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
        cwl_work_path=cwl_work_path
    )

    task2 = BashOperator(
        task_id='task_2',
        bash_command='echo "Hara, Congratulations!"'
    )

    task0 >> task1 >> task2
