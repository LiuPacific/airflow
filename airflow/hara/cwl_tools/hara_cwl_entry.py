import cwltool.main
import yaml
from cwltool.context import RuntimeContext
import os
from airflow.hara.cwl_tools.hara_engine import controller, hara_workflow
from airflow.hara.cwl_tools.config import constants
from airflow.hara.cwl_tools.tools import cwl_log

def load_cwl_workflow(main_cwl_file_path) -> dict:
    with open(main_cwl_file_path, 'r') as file:
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


def execute_cwl(hara_runtime_context: RuntimeContext, job_file_path,
                workflow_process: hara_workflow.HaraWorkflow,
                tmpdir_prefix: str,
                tmp_outdir_prefix: str,
                stagedir : str,
                outdir: str,
                basedir: str,
                run_id: str,
                file_kv_path: str,
                step_to_run: str,
                is_separate_mode: bool,
                job_content: dict
                ):

    cwl_log.get_cwl_logger().info("tmpdir_prefix: %s", tmpdir_prefix)
    cwl_log.get_cwl_logger().info("tmp_outdir_prefix: %s", tmp_outdir_prefix)
    cwl_log.get_cwl_logger().info("stagedir: %s", stagedir)
    cwl_log.get_cwl_logger().info("outdir: %s", outdir)
    cwl_log.get_cwl_logger().info("basedir: %s", basedir)
    cwl_log.get_cwl_logger().info("run_id: %s", run_id)
    cwl_log.get_cwl_logger().info("file_kv_path: %s", file_kv_path)
    cwl_log.get_cwl_logger().info("step_to_run: %s", step_to_run)
    cwl_log.get_cwl_logger().info("is_separate_mode: %s", is_separate_mode)
    cwl_log.get_cwl_logger().info("job_content: %s", job_content)

    runtime_context = hara_runtime_context.copy()
    runtime_context.basedir = basedir
    # runtime_context.outdir = os.getcwd()

    runtime_context.tmpdir_prefix = tmpdir_prefix
    runtime_context.tmp_outdir_prefix = tmp_outdir_prefix
    # runtime_context.tmpdir = '/home/typingliu/temp/tmpdir/'
    runtime_context.stagedir = stagedir
    runtime_context.outdir = outdir
    if not os.path.exists(outdir):
        os.makedirs(outdir)
    runtime_context.default_stderr = open(os.path.join(outdir, 'error.log'), 'w')
    runtime_context.default_stdout = open(os.path.join(outdir, 'stdout.log'), 'w')

    constants.init_hara_context(step_to_run, run_id, is_separate_mode, file_kv_path)

    if job_content is None:
        # Load job parameters from a YAML or JSON file
        with open(job_file_path) as job_params:
            job_content = yaml.safe_load(job_params)

    # h_executor = SingleJobExecutor()
    hara_cwl_engine = controller.HaraCwlEngine()
    out, status = hara_cwl_engine.hara_execute(process=workflow_process, job_order_object=job_content,
                                               runtime_context=runtime_context)
    # out, status = h_executor(workflow_process, job_file_items, runtime_context=runtime_context)
    if status != "success":
        raise controller.HaraWorkflowStatus(out, status)
    else:
        return out



