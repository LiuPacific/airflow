import json
import unittest
from airflow.hara.cwl_tools.config import constants
from airflow.hara.cwl_tools.tools import cwl_log
from airflow.hara.common_tools import path_tools


class TestPathTool(unittest.TestCase):
    def setUp(self):
        self.cwl_logger = cwl_log.get_cwl_logger()
        self.cwl_logger.info('start')

    def test_convert_to_valid_filename(self):
        # Example usage:
        filename = "invalid+:file.nameABC/with*special|chars?.txt"
        valid_filename = path_tools.convert_to_valid_filename(filename)
        print(f"Original filename: {filename}")
        print(f"Valid filename: {valid_filename}")  # invalid__file.nameABC_with_special_chars_.txt

    def test_directory_tree(self):
        directory_path = '/home/typingliu/temp'
        tree_dict = path_tools.directory_tree(directory_path)
        print(json.dumps(tree_dict))

    def test_directory_tree_with_URL(self):
        dagrun_id = 'manual__2024-05-24T19:17:40.498411+00:00'
        path_safe_dagrun_id = path_tools.convert_to_valid_filename(dagrun_id)
        directory_path = f'/home/typingliu/temp/{path_safe_dagrun_id}'
        tree_dict = path_tools.directory_tree(directory_path)
        print(json.dumps(tree_dict))
        '''
        {
            "tmp_outdird2f3v6mo": {},
            "tmp_outdirbmkhe6gg": {},
            "tmp_outdir": {
                "dataset_extraction_1": {
                    "bugr-bbfr.csv": "/manual__2024-05-24T19_17_40.498411_00_00/tmp_outdir/dataset_extraction_1/bugr-bbfr.csv"
                }
            },
            "outdir": {
                "error.log": "/manual__2024-05-24T19_17_40.498411_00_00/outdir/error.log",
                "stdout.log": "/manual__2024-05-24T19_17_40.498411_00_00/outdir/stdout.log"
            },
            "hara_kv_db.json": "/manual__2024-05-24T19_17_40.498411_00_00/hara_kv_db.json"
        }
        '''

