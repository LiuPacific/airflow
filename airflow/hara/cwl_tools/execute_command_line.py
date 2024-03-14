import yaml
from cwltool.context import LoadingContext, RuntimeContext
from cwltool.factory import Factory
from cwltool.executors import SingleJobExecutor
from cwltool.load_tool import load_tool


def load_cwl_workflow(cwl_file_path):
    with open(cwl_file_path, 'r') as file:
        cwl_workflow_config_dict = yaml.safe_load(file)
    return cwl_workflow_config_dict

def get_command_line_tools(cwl_workflow_config_dict):
    steps_config_dict = cwl_workflow_config_dict['steps']
    command_line_tools_to_run = []
    for step_id, step in steps_config_dict.items():
        step_file_path = step['run']  # Assuming 'run' contains the CommandLineTool definition # run: hara_write.cwl.yaml
        command_line_tools_to_run.append((step_id, step_file_path))
    return command_line_tools_to_run

def execute_step(step_file_path, inputs):
    loading_context = LoadingContext()
    runtime_context = RuntimeContext()
    runtime_context.use_container = False  # Set to True if you want to use Docker containers

    # Load and execute the CommandLineTool
    command_line_tool_process = load_tool(step_file_path, loading_context)
    executor = SingleJobExecutor()
    output = executor.execute(command_line_tool_process, runtime_context, inputs)

    return output

# Main execution logic
cwl_file_path = 'path/to/your/workflow.cwl'
cwl_workflow_config_dict = load_cwl_workflow(cwl_file_path)
command_line_tools_to_run = get_command_line_tools(cwl_workflow_config_dict)

# Example: executing each step with predefined inputs
# This is a simplified example; you'll need to adjust it according to your actual inputs and workflow structure
for step_id, step_file_path in command_line_tools_to_run:
    inputs = {}  # Populate this with the actual inputs for the step
    output = execute_step(step_file_path, inputs)
    print(f"Output of step {step_id}: {output}")
