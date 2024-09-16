import json

from flask import Blueprint, jsonify, request
from requests import HTTPError, Timeout, RequestException

from airflow.plugins_manager import AirflowPlugin
from airflow.hara.cwl_tools.config import constants
from airflow.models.dagbag import DagBag
from airflow.utils.session import NEW_SESSION, provide_session
from sqlalchemy.orm.session import Session
from typing import List
import requests

from airflow.hara.common_tools import path_tools

import yaml
import os

# Define the blueprint
blueprint_file = Blueprint(
    'hara_file_blueprint',  # Name of the blueprint
    __name__,  # The name of the module where the blueprint is located
    url_prefix='/osu/file'  # URL prefix for all the endpoints in this blueprint
)


class HaraFileApiPlugin(AirflowPlugin):
    name = "hara_web_plugin_files"
    flask_blueprints = [blueprint_file]  # Register the blueprint in the plugin


@blueprint_file.after_request
def add_headers(response):
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS, DELETE, PUT'
    response.headers[
        'Access-Control-Allow-Headers'] = 'Origin, X-Requested-With, Content-Type, Accept, Authorization'
    return response


print("hara_web_plugin0 start")


@blueprint_file.route('/get_result_output_files', methods=['GET'])
def get_result_output_files():
    '''
    input dag_run id
    :return:
    '''

    dag_id = request.args.get("dag_id")
    dag_run_id = request.args.get("dag_run_id")

    temp_dir_tree_dict = path_tools.directory_tree("/home/typingliu/temp")

    path_safe_dagrun_id = path_tools.convert_to_valid_filename(dag_run_id)
    directory_path = f'/home/typingliu/temp/{path_safe_dagrun_id}'
    tree_dict = path_tools.directory_tree(directory_path, level=0, base_path=temp_dir_tree_dict)
    return jsonify(tree_dict), 200
