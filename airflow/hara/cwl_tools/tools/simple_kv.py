import json
import threading
from typing import Any
from airflow.hara.cwl_tools.tools.abstract_kv import AbstractKVDB

class SimpleFileKVDB(AbstractKVDB):
    def __init__(self, file_path: str):
        self.file_path = file_path
        self.lock = threading.Lock()  # Use multiprocessing.Lock() for multi-process scenarios

    def _load_data(self) -> dict:
        try:
            with open(self.file_path, "r") as file:
                return json.load(file)
        except (FileNotFoundError, json.JSONDecodeError):
            return {}

    def _write_data(self, data: dict):
        with open(self.file_path, "w") as file:
            json.dump(data, file, indent=4)

    def get(self, key: str) -> Any:
        with self.lock:
            data = self._load_data()
            return data.get(key)

    def set(self, key: str, value: Any):
        with self.lock:
            data = self._load_data()
            data[key] = value
            self._write_data(data)

    def delete(self, key: str):
        with self.lock:
            data = self._load_data()
            if key in data:
                del data[key]
                self._write_data(data)

## hara TODO: the locking mechanism remains to upgrade. it couldn't avoid the unexpected writting between load and writing in this program.
if __name__ == '__main__':
    # Usage example
    kvdb = SimpleFileKVDB("/home/typingliu/temp/hara_kv_db.json")
    kvdb.set("key1", "value1")
    print(kvdb.get("key1"))
    kvdb.delete("key1")
    print(kvdb.get("key1"))
