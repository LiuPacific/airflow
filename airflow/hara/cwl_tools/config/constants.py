from airflow.hara.cwl_tools.tools.abstract_kv import AbstractKVDB
from airflow.hara.cwl_tools.tools.simple_kv import SimpleFileKVDB


class HaraContext:
    def __init__(self,
                 step_to_run: str,
                 run_id: str,
                 kvdb: AbstractKVDB,
                 is_final_step: bool = False,
                 is_separate_mode: bool = False,
                 ) -> None:
        self.step_to_run = step_to_run
        self.is_final_step = is_final_step
        self.is_separate_mode = is_separate_mode
        self.run_id = run_id
        self.kvdb = kvdb


hara_context: HaraContext = None


def init_hara_context(step_to_run: str,
                      run_id: str,
                      is_final_step: bool,
                      is_separate_mode: bool,
                      file_kv_path: str):
    global hara_context
    hara_context = HaraContext(step_to_run, run_id, SimpleFileKVDB(file_kv_path), is_final_step, is_separate_mode)


def get_hara_context() -> HaraContext:
    global hara_context
    if hara_context is None:
        raise "hara context is None"
    return hara_context
