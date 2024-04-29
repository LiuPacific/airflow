import json

from flask import Blueprint, jsonify, request
from airflow.plugins_manager import AirflowPlugin
from airflow.hara.cwl_tools.config import constants
import os

# Define the blueprint
blueprint0 = Blueprint(
    'hara_blueprint0',  # Name of the blueprint
    __name__,         # The name of the module where the blueprint is located
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

    return jsonify({'message': f'{len(files)} files have been uploaded successfully.'}), 200


class MyApiPlugin(AirflowPlugin):
    name = "hara_web_plugin0"
    flask_blueprints = [blueprint0]  # Register the blueprint in the plugin

# This function will be registered in the global namespace of this module
def some_function():
    print("Hello from some_function!")

