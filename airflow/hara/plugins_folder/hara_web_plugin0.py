import json

from flask import Blueprint, jsonify, request

from airflow.models.hara_serialized_dag import HaraSerializedDagModel
from airflow.plugins_manager import AirflowPlugin
from airflow.hara.cwl_tools.config import constants
from airflow.models.dagbag import DagBag
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.exceptions import AirflowException, ParamValidationError
from sqlalchemy.orm.session import Session
import requests

import yaml
import os

# Define the blueprint
blueprint0 = Blueprint(
    'hara_blueprint0',  # Name of the blueprint
    __name__,  # The name of the module where the blueprint is located
    url_prefix='/hara_pre'  # URL prefix for all the endpoints in this blueprint
)


@blueprint0.after_request
def add_headers(response):
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS, DELETE, PUT'
    response.headers[
        'Access-Control-Allow-Headers'] = 'Origin, X-Requested-With, Content-Type, Accept, Authorization'
    return response


print("hara_web_plugin0 start")


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


def get_cwl_saving_dir_path(workflow_name: str):
    return os.path.join(CWL_FOLDER, workflow_name)


def get_cwl_saving_path(workflow_name: str, cwl_file_name: str):
    return os.path.join(get_cwl_saving_dir_path(workflow_name), cwl_file_name)


@blueprint0.route('/add_cwl', methods=['POST'])
def add_cwl():
    """
    1. receive json
        - workflow_name
        - main_cwl
        - command_tool_lines
    2. get the json of workflow and commandlinetool
    3. save the json as cwl locally
    4. dynamically build the cwls to dag
    5. register the dag
    """
    if request.is_json:
        cwls_json_dict = request.get_json()
        print(type(cwls_json_dict))

        workflow_name = cwls_json_dict.get("workflow_name")
        main_cwl_dict = cwls_json_dict.get("main_cwl")
        command_line_tools_dict = cwls_json_dict.get("command_line_tools")

        cwl_saving_dir_path = get_cwl_saving_dir_path(workflow_name)
        os.makedirs(cwl_saving_dir_path, exist_ok=True)

        # save cwls locally

        main_cwl_path = get_cwl_saving_path(workflow_name, "main.cwl")
        with open(main_cwl_path, "w") as main_cwl_file:
            yaml_str = yaml.dump(main_cwl_dict, sort_keys=False)
            main_cwl_file.write(yaml_str)

        command_line_tool_paths = []
        for command_line_tool_file_name in command_line_tools_dict:
            command_line_tool_content = command_line_tools_dict.get(command_line_tool_file_name)
            command_line_tool_cwl_path = get_cwl_saving_path(workflow_name, command_line_tool_file_name)
            command_line_tool_paths.append(command_line_tool_cwl_path)
            with open(command_line_tool_cwl_path, "w") as command_line_tool_file:
                yaml_str = yaml.dump(command_line_tool_content, sort_keys=False)
                command_line_tool_file.write(yaml_str)

        dag_obj = generate_dag(workflow_name, main_cwl_dict)
        register_cwl(dag_obj)

        return jsonify({"message": "JSON received successfully!", "dag_id": dag_obj.dag_id}), 200
    else:
        return jsonify({"error": "json required"}), 400


# generate dag
def generate_dag(workflow_name: str, main_cwl_dict: dict):
    """
    1. init all steps
    2. set dependencies according to main workflow

    cwlVersion: v1.2
    class: Workflow
    inputs:
      message_for_step1: string # input to wc
    steps:
      writeMessage:
        run: hara_docker_write.cwl.yaml
        in:
          sentence_to_wc: message_for_step1
        out: [out1]
      countWords:
        run: hara_wc.cwl.yaml
        in:
          infile: writeMessage/out1
        out: [wordcount_result]
    outputs:
      workflow_result_wc:
        type: File
        outputSource: countWords/wordcount_result

    :param workflow_name:
    :param cwl_saving_path:
    :param main_cwl_dict:
    :param command_line_tools_dict:
    :return:
    """

    from airflow import DAG
    from datetime import datetime, timedelta
    from airflow.operators.bash import BashOperator
    from airflow.hara.hara_operator.cwl_local_pyoperator import CwlLocalOperator

    default_args = {
        'owner': 'harada',
        'start_date': datetime(2024, 4, 10),
        'retry_delay': timedelta(minutes=5),
    }
    dag_obj = DAG(dag_id=workflow_name, default_args=default_args, schedule_interval=None)

    cwl_work_path = '/home/typingliu/temp/'

    cwl_step_task_dict = {}
    # init all operators
    for step_id in main_cwl_dict.get("steps"):
        task_obj = CwlLocalOperator(
            task_id=step_id,  # cwltool echo.cwl.yaml --message_text="hello typing"
            main_cwl_file_path=get_cwl_saving_path(workflow_name, "main.cwl"),
            cwl_step_to_run=step_id,
            # TODO hara: there will be multiple final step, the one in charge of cleaning
            basedir=get_cwl_saving_dir_path(workflow_name),
            job_file_path=get_cwl_saving_path(workflow_name, 'job.yaml'),
            # TODO hara: at one dag run time, only one job can be run.
            # TODO hara: current one cwl corresponds to one job
            cwl_work_path=cwl_work_path,
            job_content=None,
            dag=dag_obj
        )
        cwl_step_task_dict[step_id] = task_obj
    # set dependencies
    for step_id in main_cwl_dict.get("steps"):
        step_in = main_cwl_dict.get("steps").get(step_id).get("in")
        if step_in is None:
            continue

        dependency_steps = []
        for command_line_tool_arg in step_in:
            #     in:
            #       infile: writeMessage/out1
            input_path = step_in.get(command_line_tool_arg)  # writeMessage/out1
            if input_path.count("/") != 1:
                continue

            # multiple dependencies required
            dependency_step = input_path[0:input_path.index('/')]
            cwl_step_task_dict[step_id].set_upstream(
                cwl_step_task_dict[dependency_step])
            dependency_steps.append(dependency_step)
            # TODO hara: source, output need to be considered.
        # if len(dependency_steps) > 0:
        #     cwl_step_task_dict[step_id].set_upstream(cwl_step_task_dict[dependency_steps])
        # TODO hara: final node

    return dag_obj


# to delete records in the following tables: dag, dag_pickle, serialized_dag, hara_serialized_dag,
# TODO: whether to delete log, file, task_instance, job, dagrun remains uncertain
# can be delete together, or can be an offline script doing cleaning job.
@blueprint0.route('/delete_cwl', methods=['DELETE'])
def delete_cwl():
    dag_id = request.values.get("dag_id")

    try:
        delete_cwl_db(dag_id)
        # delete files will be in a separate cleaning script: cwl, log, dag file, running files under the dag_id
    except Exception as e:
        # TODO error response: 400, 500, 404 ...
        return jsonify({"message": str(e), "dag_id": dag_id}), 500
    return jsonify({"message": "success", "dag_id": dag_id}), 200


@provide_session
def delete_cwl_db(dag_id: str, session: Session = None):
    from airflow.exceptions import DagNotFound
    from airflow.api.common import delete_dag

    try:
        session.begin()

        # perform original delete logic
        delete_dag.delete_dag(dag_id, session=session)
        if HaraSerializedDagModel.has_dag(dag_id=dag_id, session=session):
            HaraSerializedDagModel.remove_dag(dag_id=dag_id, session=session)

        session.commit()

    except DagNotFound as error:
        print(f"DAG with id {dag_id} not found. Cannot delete", "error")
        session.rollback()
        raise error
    except AirflowException as error:
        print(
            f"Cannot delete DAG with id {dag_id} because some task instances of the DAG "
            "are still running. Please mark the  task instances as "
            "failed/succeeded before deleting the DAG",
            "error",
        )
        session.rollback()
        raise error
    finally:
        session.close()

    return 0;


# job.yaml is required
# workflow_name is required
# other files can be uploaded together
@blueprint0.route('/trigger_cwl_with_jobyaml', methods=['POST'])
def trigger_cwl_with_jobyaml():
    if 'files' not in request.files:
        return jsonify({'message': 'No files part in the request'}), 400
    files = request.files.getlist('files')
    if not files or files[0].filename == '':
        return jsonify({'message': 'No selected file'}), 400

    # TODO hara: some validation should be added on the cwl name.
    dag_id = request.form.get('dag_id')
    saving_dir_path = get_cwl_saving_dir_path(dag_id)
    if not os.path.exists(saving_dir_path):
        os.makedirs(saving_dir_path)
    for file in files:
        if file.filename == '':
            return jsonify({'message': 'No selected file'}), 400

        # Save each cwl file
        file_path = os.path.join(saving_dir_path, file.filename)
        file.save(file_path)

    trigger_response_dict = request_trigger(dag_id, None)

    return jsonify(trigger_response_dict), 200


@blueprint0.route('/trigger_cwl', methods=['POST'])
def trigger_cwl():
    '''
    receive json:
        {
            "dag_id": "fdsaf",
            "job_content":{
                "message_for_step1": "Kyoto Osaka Fukuoka Osaka Nagoya"
            }
        }
    :return:
    '''

    if not request.is_json:
        return jsonify({"error": "json required"}), 400
    else:
        trigger_json = request.get_json()
        dag_id = trigger_json.get("dag_id")
        job_content = trigger_json.get("job_content")

        # unpause
        unpause_dag(dag_id)
        # trigger
        trigger_response_dict = request_trigger(dag_id, job_content)
        return jsonify(trigger_response_dict), 200


def unpause_dag(dag_id: str):
    url_for_unpause = f"http://localhost:8081/paused?is_paused=true&dag_id={dag_id}"
    response = requests.post(url_for_unpause)
    response.raise_for_status()


def request_trigger(dag_id: str, job_content: dict):
    url_for_trigger = f"http://localhost:8081/api/v1/dags/{dag_id}/dagRuns"
    headers_dict = {
        "Content-Type": "application/json",
        # "Authorization": "ACCESS_TOKEN"
    }
    json_data_dict = {
        "conf": {"job_content": job_content}
    }

    try:
        response = requests.post(url_for_trigger, headers=headers_dict, json=json_data_dict)
        response.raise_for_status()
        print(f"Status code: {response.status_code}")
        print("Response JSON:", response.json())

        '''
        {
            "conf": {
                "param1": "value1",
                "param2": "value2"
            },
            "dag_id": "inside_controlled_docker_cwl_dag9",
            "dag_run_id": "manual__2024-05-16T04:32:01.758290+00:00",
            "data_interval_end": "2024-05-16T04:32:01.758290+00:00",
            "data_interval_start": "2024-05-16T04:32:01.758290+00:00",
            "end_date": null,
            "execution_date": "2024-05-16T04:32:01.758290+00:00",
            "external_trigger": true,
            "last_scheduling_decision": null,
            "logical_date": "2024-05-16T04:32:01.758290+00:00",
            "note": null,
            "run_type": "manual",
            "start_date": null,
            "state": "queued"
        }
        '''
        return response.json()
    # except HTTPError as http_err:
    #     print(f"HTTP error occurred: {http_err}")
    # except Timeout as timeout_err:
    #     print(f"Request timed out: {timeout_err}")
    # except RequestException as req_err:
    #     print(f"Request exception occurred: {req_err}")
    except Exception as err:
        print(f"An error occurred: {err}")
        raise err


# it's a test for cwl with commandlinetool
@blueprint0.route('/add_cwl_hardcode', methods=['POST'])
def add_cwl_hardcode():
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
    dag_obj = hara_add_dag()
    register_cwl(dag_obj=dag_obj)
    return jsonify({'message': f'{len(files)} files have been uploaded successfully.'}), 200


@provide_session
def register_cwl(dag_obj, session: Session = NEW_SESSION):
    print('hara register dag start')
    dag_dir = '/home/typingliu/workspace/tpy/airflow25/airflow/airflow/hara/dags_folder'
    dagbag = DagBag(dag_dir, include_examples=False)

    dagbag.dags[dag_obj.dag_id] = dag_obj

    dagbag.sync_to_db(processor_subdir=dag_dir, session=session)

    session.commit()
    for dag_id, dag in dagbag.dags.items():
        dag.pickle(session)
        session.commit()


from airflow.models import DagModel


@blueprint0.route('/dags', methods=['GET'])
def dags():
    dags_dict = list_dags(is_active=True)
    return dags_dict, 200


@provide_session
def list_dags(is_active: bool, session: Session = NEW_SESSION):
    dags = session.query(DagModel).filter_by(is_active=is_active).all()

    dags_dict = {}
    for dag in dags:
        dags_dict[dag.dag_id] = {"is_active": dag.is_active, "is_paused": dag.is_paused, "owner": dag.owners}
    return dags_dict


class MyApiPlugin(AirflowPlugin):
    name = "hara_web_plugin0"
    flask_blueprints = [blueprint0]  # Register the blueprint in the plugin


def hara_add_dag():
    from airflow import DAG
    from datetime import datetime, timedelta
    from airflow.operators.bash import BashOperator
    from airflow.hara.hara_operator.cwl_local_pyoperator import CwlLocalOperator

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

    dag_id = f'inside_controlled_docker_cwl_dag6'
    dag_obj = create_dag(dag_id, owner='harada', start_date=datetime(2024, 4, 10), retry_delay_minutes=5)
    return dag_obj
