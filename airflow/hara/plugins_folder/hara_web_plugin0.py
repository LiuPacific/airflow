import json

from flask import Blueprint, jsonify, request
from airflow.plugins_manager import AirflowPlugin
from airflow.hara.cwl_tools.config import constants
from airflow.models.dagbag import DagBag
from airflow.utils.session import NEW_SESSION, provide_session
from sqlalchemy.orm.session import Session
import os

# Define the blueprint
blueprint0 = Blueprint(
    'hara_blueprint0',  # Name of the blueprint
    __name__,  # The name of the module where the blueprint is located
    url_prefix='/hara_pre'  # URL prefix for all the endpoints in this blueprint
)

print("aa")


# Define an endpoint
@blueprint0.route('/hara', methods=['GET'])
def hello():
    return jsonify({'message': 'Hello Hara san!'})


@blueprint0.route('/hara0', methods=['GET'])
def hello0():
    return jsonify({'message': constants.a_test})


#
'''
curl -X POST \
  -F "files=@/home/typingliu/workspace/tpy/airflow25/airflow/airflow/hara/hara_dags/cwl_2docker_nodes_dag/hara_docker_write.cwl.yaml" \
  -F "files=@/home/typingliu/workspace/tpy/airflow25/airflow/airflow/hara/hara_dags/cwl_2docker_nodes_dag/hara_job.yaml" \
  -F "files=@/home/typingliu/workspace/tpy/airflow25/airflow/airflow/hara/hara_dags/cwl_2docker_nodes_dag/hara_wc.cwl.yaml" \
  -F "files=@/home/typingliu/workspace/tpy/airflow25/airflow/airflow/hara/hara_dags/cwl_2docker_nodes_dag/main.cwl.yaml" \
  -F "cwl_name=cwl_2node_test0" \
  http://127.0.0.1:8081/hara_pre/add_cwl


curl --location 'http://127.0.0.1:8081/hara_pre/add_cwl' \
--form 'files=@"/home/typingliu/cwl_demo/hara_docker_write.cwl.yaml"' \
--form 'files=@"/home/typingliu/cwl_demo/hara_wc.cwl.yaml"' \
--form 'cwl_name="dfs"'
'''
#
CWL_FOLDER = '/home/typingliu/cwl_demo/'


# cwl with commandlinetool
@blueprint0.route('/add_cwl', methods=['POST'])
def add_cwl():
    if 'files' not in request.files:
        return jsonify({'message': 'No files part in the request'}), 400
    files = request.files.getlist('files')
    if not files or files[0].filename == '':
        return jsonify({'message': 'No selected file'}), 400

    # TODO hara: some validation should be added on the cwl name.
    cwl_name = request.form.get('cwl_name')
    saving_dir_path = os.path.join(CWL_FOLDER, cwl_name)
    if not os.path.exists(saving_dir_path):
        os.makedirs(saving_dir_path)
    for file in files:
        if file.filename == '':
            return jsonify({'message': 'No selected file'}), 400

        # Save each cwl file
        file_path = os.path.join(saving_dir_path, file.filename)
        file.save(file_path)

    register_cwl()
    return jsonify({'message': f'{len(files)} files have been uploaded successfully.'}), 200


@provide_session
def register_cwl(session: Session = NEW_SESSION, ):
    print('hara register dag start')
    dag_dir = '/home/typingliu/workspace/tpy/airflow25/airflow/airflow/hara/dags_folder'
    dagbag = DagBag(dag_dir, include_examples=False)

    dag_obj = hara_add_dag()
    dagbag.dags[dag_obj.dag_id] = dag_obj

    dagbag.sync_to_db(processor_subdir=dag_dir, session=session)

    session.commit()
    for dag_id, dag in dagbag.dags.items():
        dag.pickle(session)
        session.commit()


class MyApiPlugin(AirflowPlugin):
    name = "hara_web_plugin0"
    flask_blueprints = [blueprint0]  # Register the blueprint in the plugin


def hara_add_dag():
    from airflow import DAG
    from datetime import datetime, timedelta
    from airflow.operators.bash import BashOperator
    from airflow.hara.hara_operator.cwl_local_pyoperator import CwlLocalOperator

    cwl_file_path = "/home/typingliu/workspace/tpy/airflow25/airflow/airflow/hara/hara_dags/cwl_2docker_nodes_dag/main.cwl.yaml"
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
            cwl_file_path=cwl_file_path,
            cwl_step_to_run='writeMessage',
            is_final_step=False,
            basedir=basedir,
            job_file_path=job_file_path,
            cwl_work_path=cwl_work_path,
            dag=dag_obj
        )

        task2 = CwlLocalOperator(
            task_id='task_2',  # cwltool echo.cwl.yaml --message_text="hello typing"
            cwl_file_path=cwl_file_path,
            cwl_step_to_run='countWords',
            is_final_step=True,
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

    dag_id = f'inside_controlled_docker_cwl_dag6'
    dag_obj = create_dag(dag_id, owner='harada', start_date=datetime(2024, 4, 10), retry_delay_minutes=5)
    return dag_obj
