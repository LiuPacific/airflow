from airflow.hara.cwl_tools.config import constants


def get_task_info_key(step_name) -> str:
    return constants.get_hara_context().run_id + '_task_info_' + step_name


def set_task_info(step_name, task_status):
    if task_status != 'success':
        raise 'now only success is supported. The function remains to be design dedicately'
    task_info_key = get_task_info_key(step_name)
    task_info = {'task_tatus': task_status}
    constants.get_hara_context().kvdb.set(task_info_key, task_info)


def get_task_info(step_name):
    task_info_key = get_task_info_key(step_name)
    task_info = constants.get_hara_context().kvdb.get(task_info_key)
    if task_info is None:
        task_info = {}
    return task_info


def get_completed_key() -> str:
    return constants.get_hara_context().run_id + '_completed_num'


def get_node_completed_num():
    completed_num = constants.get_hara_context().kvdb.get(get_completed_key())
    if completed_num is None:
        completed_num = 0
    return completed_num


def add_node_completed_num():
    completed_num = get_node_completed_num()
    completed_num = completed_num + 1
    constants.get_hara_context().kvdb.set(get_completed_key(), completed_num)
