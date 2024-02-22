import cwltool.factory
import cwltool.main
import yaml


def execute_cwl(cwl_file, job_file):
    # Load the CWL workflow
    fac = cwltool.factory.Factory()

    # Create an executable workflow object from CWL file
    cwlexec = fac.make(cwl_file)

    # Load job parameters from a YAML or JSON file
    with open(job_file) as job_params:
        job = yaml.safe_load(job_params)

    # Execute the workflow with the job parameters
    result = cwlexec(**job)
    # result = cwlexec(message_for_step1="Kyoto Osaka Fukuoka Osaka Nagoya")

    # Print the execution result
    print("Execution result:", result)

if __name__ == "__main__":
    # Path to your CWL file
    # cwl_file_path = "/home/typingliu/workspace/tpy/airflow21/airflow/airflow/hara_bin/cwl_tools/echo.cwl.yaml"
    cwl_file_path = "rw_example/hara_workflow.cwl.yaml"
    # Path to your job file (YAML or JSON format) with input parameters for the workflow
    # job_file_path = "/home/typingliu/workspace/tpy/airflow21/airflow/airflow/hara_bin/cwl_tools/echo.cwl.job.yaml"
    job_file_path = "rw_example/hara_job.yaml"

    # Execute the CWL workflow
    execute_cwl(cwl_file_path, job_file_path)
