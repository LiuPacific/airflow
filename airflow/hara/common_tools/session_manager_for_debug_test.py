import unittest
from sqlalchemy.orm import Session
from airflow.hara.cwl_tools.tools import cwl_log
from airflow.hara.common_tools.session_manager_for_debug import provide_session_for_debug
from airflow.models.hara_serialized_dag import HaraSerializedDagModel


class SessionManagerDebugTest(unittest.TestCase):
    def setUp(self):
        self.cwl_logger = cwl_log.get_cwl_logger()
        self.cwl_logger.info('start')

    def test_get_hara_serialized_dag(self):
        self.get_hara_serialized_dag()

    @provide_session_for_debug
    def get_hara_serialized_dag(self, session: Session = None):
        self.cwl_logger.info(session)
        dags = HaraSerializedDagModel.read_all_dags(session=session)
        self.cwl_logger.info(dags)
