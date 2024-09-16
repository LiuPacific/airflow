from airflow.hara.cwl_tools.config import constants
from airflow.hara.cwl_tools.tools import cwl_log
from airflow.hara.cwl_tools.tools.lock_wrapper import resource_lock_decorator

def get_task_info_key(step_name) -> str:
    return constants.get_hara_context().run_id + '_task_info_' + step_name


@resource_lock_decorator('/home/typingliu/temp/kv.lock')
def set_task_info(step_name, task_status):
    if task_status != 'success':
        raise 'now only success is supported. The function remains to be design dedicately'
    task_info_key = get_task_info_key(step_name)
    task_info = {'task_status': task_status}
    constants.get_hara_context().kvdb.set(task_info_key, task_info)

@resource_lock_decorator('/home/typingliu/temp/kv.lock')
def get_task_info(step_name):
    task_info_key = get_task_info_key(step_name)
    task_info = constants.get_hara_context().kvdb.get(task_info_key)
    if task_info is None:
        task_info = {}
    return task_info


def get_completed_key() -> str:
    return constants.get_hara_context().run_id + '_completed_num'


@resource_lock_decorator('/home/typingliu/temp/kv.lock')
def add_node_completed_num():
    completed_num = get_node_completed_num()
    completed_num = completed_num + 1
    constants.get_hara_context().kvdb.set(get_completed_key(), completed_num)

@resource_lock_decorator('/home/typingliu/temp/kv.lock')
def get_node_completed_num() -> int:
    cwl_log.get_cwl_logger().info("get_node_completed_num: %s", get_completed_key())
    completed_num = constants.get_hara_context().kvdb.get(get_completed_key())
    if completed_num is None:
        completed_num = 0
    return completed_num


def get_step_num_key() -> str:
    return constants.get_hara_context().run_id + '_step_num'

@resource_lock_decorator('/home/typingliu/temp/kv.lock')
def get_workflow_step_num() -> int:
    workflow_step_num = constants.get_hara_context().kvdb.get(get_step_num_key())
    if workflow_step_num is None:
        raise "workflow_step_num is None"
    return workflow_step_num

@resource_lock_decorator('/home/typingliu/temp/kv.lock')
def set_workflow_step_num(workflow_step_num:int):
    constants.get_hara_context().kvdb.set(get_step_num_key(), workflow_step_num)
