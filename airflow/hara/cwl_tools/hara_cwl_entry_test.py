import unittest
from airflow.hara.cwl_tools.config import constants
from airflow.hara.cwl_tools.tools import cwl_log
from airflow.hara.cwl_tools.hara_engine import controller, hara_workflow
from airflow.hara.cwl_tools import hara_cwl_entry
import yaml
from cwltool.context import RuntimeContext
import os


class TestSeparatedCwl(unittest.TestCase):
    def setUp(self):
        self.cwl_logger = cwl_log.get_cwl_logger()
        self.cwl_logger.info('start')

    def test_execute_cwl_rw_example(self):
        # Path to your CWL file
        # cwl_file_path = "/home/typingliu/workspace/tpy/airflow21/airflow/airflow/hara_bin/cwl_tools/echo.cwl.yaml"
        cwl_file_path = "rw_example/hara_workflow.cwl.yaml"
        # Path to your job file (YAML or JSON format) with input parameters for the workflow
        # job_file_path = "/home/typingliu/workspace/tpy/airflow21/airflow/airflow/hara_bin/cwl_tools/echo.cwl.job.yaml"
        job_file_path = "rw_example/hara_job.yaml"

        hara_cwl_engine = controller.HaraCwlEngine()
        workflow_process = hara_cwl_engine.load_configuration(cwl_file_path)

        # Execute the CWL workflow
        tmpdir_prefix = '/home/typingliu/temp/'
        tmp_outdir_prefix = '/home/typingliu/temp/'
        # runtime_context.tmpdir = '/home/typingliu/temp/tmpdir/'
        stagedir = '/home/typingliu/temp/stagedir/'
        outdir = '/home/typingliu/temp/outdir/'
        basedir = '/home/typingliu/workspace/tpy/airflow25/airflow/airflow/hara/cwl_tools/rw_example'

        run_id = 'abc'
        file_kv_path = '/home/typingliu/temp/hara_kv_db.json'

        # step_to_run = 'writeMessage';
        # is_final_step = False;
        # is_separate_mode = True;
        step_to_run = 'countWords';
        is_final_step = True;
        is_separate_mode = True;

        hara_cwl_entry.execute_cwl(hara_cwl_engine.h_runtime_context, job_file_path,
                                   workflow_process=workflow_process,
                                   tmpdir_prefix=tmpdir_prefix, tmp_outdir_prefix=tmp_outdir_prefix,
                                   stagedir=stagedir, basedir=basedir, outdir=outdir,
                                   run_id=run_id, file_kv_path=file_kv_path,
                                   step_to_run=step_to_run, is_final_step=is_final_step,
                                   is_separate_mode=is_separate_mode
                                   )
        self.cwl_logger.info('finished')

    def test_docker_nodes(self):
        # Path to your CWL file
        cwl_file_path = "/home/typingliu/workspace/tpy/airflow25/airflow/airflow/hara/hara_dags/cwl_2docker_nodes_dag/main.cwl.yaml"
        job_file_path = "/home/typingliu/workspace/tpy/airflow25/airflow/airflow/hara/hara_dags/cwl_2docker_nodes_dag/hara_job.yaml"

        hara_cwl_engine = controller.HaraCwlEngine()
        workflow_process = hara_cwl_engine.load_configuration(cwl_file_path)

        run_id = 'abc'
        # Execute the CWL workflow
        tmpdir_prefix = os.path.join('/home/typingliu/temp/', run_id, 'tmp_outdir')
        tmp_outdir_prefix = os.path.join('/home/typingliu/temp/', run_id)
        # runtime_context.tmpdir = '/home/typingliu/temp/tmpdir/'
        stagedir = os.path.join('/home/typingliu/temp/', run_id, 'stagedir/')
        outdir = os.path.join('/home/typingliu/temp/', run_id, 'outdir/')
        basedir = '/home/typingliu/workspace/tpy/airflow25/airflow/airflow/hara/cwl_tools/rw_example'

        file_kv_path = os.path.join('/home/typingliu/temp/', run_id, 'hara_kv_db.json')

        # step_to_run = 'writeMessage';
        # is_final_step = False;
        # is_separate_mode = True;
        step_to_run = 'countWords';
        is_final_step = True;
        is_separate_mode = True;

        hara_cwl_entry.execute_cwl(hara_cwl_engine.h_runtime_context, job_file_path,
                                   workflow_process=workflow_process,
                                   tmpdir_prefix=tmpdir_prefix, tmp_outdir_prefix=tmp_outdir_prefix,
                                   stagedir=stagedir, basedir=basedir, outdir=outdir,
                                   run_id=run_id, file_kv_path=file_kv_path,
                                   step_to_run=step_to_run, is_final_step=is_final_step,
                                   is_separate_mode=is_separate_mode
                                   )
        self.cwl_logger.info('finished')
