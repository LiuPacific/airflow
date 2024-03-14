import json
import os.path
import threading
from typing import Any
from airflow.hara.cwl_tools.tools.abstract_kv import AbstractKVDB
from airflow.hara.cwl_tools.tools import cwl_log
from airflow.hara.cwl_tools.tools.lock_wrapper import resource_lock_decorator


class SimpleFileKVDB(AbstractKVDB):
    @resource_lock_decorator('/home/typingliu/temp/kv.lock')
    def __init__(self, file_path: str):
        self.file_path = file_path
        self.lock = threading.Lock()  # Use multiprocessing.Lock() for multi-process scenarios
        dir_path = os.path.dirname(file_path)
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
        if not os.path.exists(file_path):
            with open(file_path, 'w') as file:
                file.write('{}')

    @resource_lock_decorator('/home/typingliu/temp/kv.lock')
    def _load_data(self) -> dict:
        try:
            with open(self.file_path, "r") as file:
                print("---_load_data")
                data_txt = file.read()
                cwl_log.get_cwl_logger().info("file_kv data text: #%s#", data_txt)

                data = json.loads(data_txt)
                cwl_log.get_cwl_logger().info("file_kv data: #%s#", data)
                # data = json.load(data)
                # cwl_log.get_cwl_logger().info("file_kv data: %s", data)
                return data
        except FileNotFoundError as fileNotFoundError:
            cwl_log.get_cwl_logger().info("_load_data %s not found", self.file_path)
        except json.JSONDecodeError as jsonError:
            with open(self.file_path, "r") as file:
                data_txt = file.read()
                cwl_log.get_cwl_logger().info("json error: file is #%s#", data_txt)
                cwl_log.get_cwl_logger().error(jsonError)
        return {}

    @resource_lock_decorator('/home/typingliu/temp/kv.lock')
    def _write_data(self, data: dict):
        with open(self.file_path, "w") as file:
            cwl_log.get_cwl_logger().info("_write_data is #%s#", data)
            json.dump(data, file, indent=4)
            # data_text = json.dumps(data)
            # file.write(data_text)
            # file.flush()
            # os.fsync(file.fileno())

    @resource_lock_decorator('/home/typingliu/temp/kv.lock')
    def get(self, key: str) -> Any:
        with self.lock:
            data = self._load_data()
            return data.get(key)

    @resource_lock_decorator('/home/typingliu/temp/kv.lock')
    def set(self, key: str, value: Any):
        with self.lock:
            data = self._load_data()
            data[key] = value
            self._write_data(data)

    @resource_lock_decorator('/home/typingliu/temp/kv.lock')
    def delete(self, key: str):
        with self.lock:
            data = self._load_data()
            if key in data:
                del data[key]
                self._write_data(data)


## hara TODO: the locking mechanism remains to upgrade. it couldn't avoid the unexpected writting between load and writing in this program.
# if __name__ == '__main__':
#     # Usage example
#     kvdb = SimpleFileKVDB("/home/typingliu/temp/hara_kv_db.json")
#     kvdb.set("key1", "value1")
#     print(kvdb.get("key1"))
#     kvdb.delete("key1")
#     print(kvdb.get("key1"))

if __name__ == '__main__':
    import json

    with open('/home/typingliu/workspace/tpy/airflow25/airflow/airflow/hara/config/1.json', "r") as file:
        data = json.load(file)
        print("---asd")
        cwl_log.get_cwl_logger().info("file_kv data: %s", data)
        print(data)
