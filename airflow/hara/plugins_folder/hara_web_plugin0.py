import json

from flask import Blueprint, jsonify
from airflow.plugins_manager import AirflowPlugin
from airflow.hara.cwl_tools.config import constants

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


class MyApiPlugin(AirflowPlugin):
    name = "hara_web_plugin0"
    flask_blueprints = [blueprint0]  # Register the blueprint in the plugin

# This function will be registered in the global namespace of this module
def some_function():
    print("Hello from some_function!")

