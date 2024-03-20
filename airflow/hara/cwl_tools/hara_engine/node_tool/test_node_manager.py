import unittest

from airflow.hara.cwl_tools.hara_engine.node_tool import node_manager
from airflow.hara.cwl_tools.config import constants

class TestNodeManager(unittest.TestCase):
    def setUp(self):
        constants.init_hara_context(step_to_run='countWords',
                                    run_id='manual__2024-05-13T03_03_39.563737_00_00',
                                    is_separate_mode=True,
                                    file_kv_path='/home/typingliu/temp/manual__2024-05-13T03_03_39.563737_00_00/hara_kv_db.json')
    def test_add_node_completed_num(self):
        n = node_manager.get_node_completed_num()
        print(n)
