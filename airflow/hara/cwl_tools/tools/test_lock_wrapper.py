import unittest
import os
from airflow.hara.cwl_tools.tools.lock_wrapper import resource_lock_decorator
import time



class TestLockWrapper(unittest.TestCase):
    def setUp(self):
        print('hara test wrapper start')

    @resource_lock_decorator("./ha.lock")
    def process_files(self, file_path):
        if os.path.exists(file_path):
            with open(file_path, 'r+') as f:
                data = f.read()
                # Simulate some processing (e.g., modifying the content)
                time.sleep(3)
                print(f"Processing {file_path}: {data}")
        else:
            print(f"File {file_path} does not exist.")
        self.another_process_files(file_path)

    @resource_lock_decorator("./ha.lock")
    def another_process_files(self, file_path):
        if os.path.exists(file_path):
            with open(file_path, 'r+') as f:
                data = f.read()
                # Simulate some processing (e.g., modifying the content)
                time.sleep(3)
                print(f"Processing {file_path}: {data}")
        else:
            print(f"File {file_path} does not exist.")

    def test1(self):
        self.process_files('./ha.lock')

    def test2(self):
        self.process_files('./ha.lock')
