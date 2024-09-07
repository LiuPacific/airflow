from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator

def create_dag(dag_id, owner, start_date, retry_delay_minutes):
    default_args = {
        'owner': owner,
        'start_date': start_date,
        'retry_delay': timedelta(minutes=retry_delay_minutes),
    }

    dag_obj = DAG(dag_id=dag_id, default_args=default_args, schedule_interval=None)

    # Create tasks and explicitly assign them to the DAG
    task1 = BashOperator(
        task_id='task_1',
        bash_command='echo "Hello Harada!"',
        dag=dag_obj
    )
    task2 = BashOperator(
        task_id='task_2',
        bash_command='echo "my second operator"',
        dag=dag_obj
    )

    task3 = BashOperator(
        task_id='task_3',
        bash_command='echo "my third operator"',
        dag=dag_obj
    )

    task4 = BashOperator(
        task_id='task_4',
        bash_command='echo "my third operator"',
        dag=dag_obj
    )

    task5 = BashOperator(
        task_id='task_5',
        bash_command='echo "my third operator"',
        dag=dag_obj
    )

    task2.set_upstream(task1)
    task3.set_upstream(task1)
    task4.set_upstream(task2)
    task4.set_upstream(task3)
    # task1 >> task2 >> task3
    # task2 >> task3
    return dag_obj


dag_id = f'dynamic_once_dag0'
dag_obj = create_dag(dag_id, owner='harada', start_date=datetime(2024, 4, 10), retry_delay_minutes=5)
globals()[dag_id] = dag_obj

