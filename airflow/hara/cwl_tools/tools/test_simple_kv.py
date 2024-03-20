import unittest

from airflow.hara.cwl_tools.tools.simple_kv import SimpleFileKVDB


class TestSimpleKV(unittest.TestCase):
    def setUp(self):
        self.kvdb = SimpleFileKVDB("/home/typingliu/temp/hara_kv_db.json")

    def test_set_get_str(self):
        self.kvdb.set("key1", "value1")
        v = self.kvdb.get("key1")
        print(v)
        self.kvdb.delete("key1")

    def test_set_get_num(self):
        hara_completed_num = self.kvdb.get("hara_completed_num")
        if hara_completed_num is None:
            hara_completed_num = 0
        hara_completed_num = hara_completed_num + 1
        self.kvdb.set("hara_completed_num", hara_completed_num)
        hara_completed_num = self.kvdb.get("hara_completed_num")
        print(hara_completed_num)

    def test_set_get_multi_layer(self):
        node_status_node1 = self.kvdb.get("node_status_node1")
        if node_status_node1 is None:
            node_status_node1 = {}
        node_status_node1 = {'status':'success'}
        self.kvdb.set("node_status_node1", node_status_node1)
        node_status_node1 = self.kvdb.get("node_status_node1")
        print(node_status_node1)

    def test_get_str(self):
        v = self.kvdb.get("key1")
        print(v)

    def test_delete(self):
        self.kvdb.delete("key1")
