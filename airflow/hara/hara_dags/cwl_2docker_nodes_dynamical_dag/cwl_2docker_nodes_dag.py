from airflow import DAG

from datetime import datetime, timedelta
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.hara.hara_operator.cwl_local_pyoperator import CwlLocalOperator

default_args = {
    'owner': 'harada',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

main_cwl_file_path = "/home/typingliu/workspace/tpy/airflow25/airflow/airflow/hara/hara_dags/cwl_2docker_nodes_dag/main.cwl.yaml"
job_file_path = "/home/typingliu/workspace/tpy/airflow25/airflow/airflow/hara/hara_dags/cwl_2docker_nodes_dag/hara_job.yaml"
basedir = '/home/typingliu/workspace/tpy/airflow25/airflow/airflow/hara/hara_dags/cwl_2docker_nodes_dag'
cwl_work_path = '/home/typingliu/temp/'

def create_dag(dag_id, owner, start_date, retry_delay_minutes):
    default_args = {
        'owner': owner,
        'start_date': start_date,
        'retry_delay': timedelta(minutes=retry_delay_minutes),
    }

    dag_obj = DAG(dag_id=dag_id, default_args=default_args, schedule_interval=None)

    # Create tasks and explicitly assign them to the DAG
    task0 = BashOperator(
        task_id='task_0',
        bash_command='echo "hara Start"',
        dag=dag_obj
    )

    task1 = CwlLocalOperator(
        task_id='task_1',  # cwltool echo.cwl.yaml --message_text="hello typing"
        main_cwl_file_path=main_cwl_file_path,
        cwl_step_to_run='writeMessage',
        basedir=basedir,
        job_file_path=job_file_path,
        cwl_work_path=cwl_work_path,
        dag=dag_obj
    )

    task2 = CwlLocalOperator(
        task_id='task_2',  # cwltool echo.cwl.yaml --message_text="hello typing"
        main_cwl_file_path=main_cwl_file_path,
        cwl_step_to_run='countWords',
        basedir=basedir,
        job_file_path=job_file_path,
        cwl_work_path=cwl_work_path,
        dag=dag_obj
    )

    task3 = BashOperator(
        task_id='task_3',
        bash_command='echo "Hara, Congratulations!"',
        dag=dag_obj
    )

    task0 >> task1 >> task2 >> task3
    # task0 >> task3
    return dag_obj

dag_id = f'controlled_docker_cwl_dag'
dag_obj = create_dag(dag_id, owner='harada', start_date=datetime(2024, 4, 10), retry_delay_minutes=5)
globals()[dag_id] = dag_obj

