from airflow.hara.cwl_tools.hara_engine import controller
import cwltool.main
import yaml
from cwltool.context import LoadingContext, RuntimeContext
import os
from cwltool.executors import JobExecutor, SingleJobExecutor
from cwltool import workflow
from cwltool.loghandler import _logger
from cwltool.process import Process
from cwltool.utils import CWLObjectType
import logging
from typing import Union, Tuple, Optional
from airflow.hara.cwl_tools.hara_engine import controller, hara_workflow
from airflow.hara.cwl_tools.hara_engine import constants

def load_cwl_workflow(cwl_file_path) -> dict:
    with open(cwl_file_path, 'r') as file:
        cwl_workflow = yaml.safe_load(file)

    return cwl_workflow


def load_job_file(job_file_path) -> dict:
    with open(job_file_path, 'r') as file:
        job_params = yaml.safe_load(file)  # {'message_for_step1': 'Kyoto Osaka Fukuoka Osaka Nagoya'}
    return job_params


def execute_cwl1(cwl_file, job_file):
    # Load the CWL workflow
    fac = cwltool.factory.Factory()

    # Create an executable workflow object from CWL file
    cwlexec = fac.make(cwl_file)

    # Load job parameters from a YAML or JSON file
    with open(job_file) as job_params:
        job = yaml.safe_load(job_params)

    # Execute the workflow with the job parameters
    result = controller.split_workflow(**job)
    # = cwlexec(**job)

    # Print the execution result
    print("Execution result:", result)


def execute_cwl(hara_runtime_context: RuntimeContext, job_file_path, workflow_process: hara_workflow.HaraWorkflow):
    runtime_context = hara_runtime_context.copy()
    runtime_context.basedir = os.getcwd()
    runtime_context.outdir = os.getcwd()

    runtime_context.tmpdir_prefix = '/home/typingliu/temp/'
    runtime_context.tmp_outdir_prefix = '/home/typingliu/temp/'
    # runtime_context.tmpdir = '/home/typingliu/temp/tmpdir/'
    runtime_context.stagedir = '/home/typingliu/temp/stagedir/'
    runtime_context.outdir = '/home/typingliu/temp/outdir/'

    run_id = 'abc'
    file_kv_path = '/home/typingliu/temp/hara_kv_db.json'
    step_to_run = 'writeMessage';
    is_final_step = False;
    is_separate_mode = False;
    # step_to_run = 'countWords';
    # is_final_step = True;
    # is_separate_mode = True;

    constants.init_hara_context(step_to_run, run_id, is_final_step, is_separate_mode, file_kv_path)

    # Load job parameters from a YAML or JSON file
    with open(job_file_path) as job_params:
        job_file_items = yaml.safe_load(job_params)

    # h_executor = SingleJobExecutor()
    hara_cwl_engine = controller.HaraCwlEngine()
    out, status = hara_cwl_engine.hara_execute(process=workflow_process, job_order_object=job_file_items,
                                               runtime_context=runtime_context)
    # out, status = h_executor(workflow_process, job_file_items, runtime_context=runtime_context)
    if status != "success":
        raise controller.HaraWorkflowStatus(out, status)
    else:
        return out


if __name__ == "__main__":
    # Path to your CWL file
    # cwl_file_path = "/home/typingliu/workspace/tpy/airflow21/airflow/airflow/hara_bin/cwl_tools/echo.cwl.yaml"
    cwl_file_path = "rw_example/hara_workflow.cwl.yaml"
    # Path to your job file (YAML or JSON format) with input parameters for the workflow
    # job_file_path = "/home/typingliu/workspace/tpy/airflow21/airflow/airflow/hara_bin/cwl_tools/echo.cwl.job.yaml"
    job_file_path = "rw_example/hara_job.yaml"

    hara_cwl_engine = controller.HaraCwlEngine()
    workflow_process = hara_cwl_engine.load_configuration(cwl_file_path)

    # Execute the CWL workflow
    execute_cwl(hara_cwl_engine.h_runtime_context, job_file_path, workflow_process=workflow_process)
