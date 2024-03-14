import os
from typing import Any, Dict, Optional, Union, Tuple, MutableMapping
from ruamel.yaml.comments import CommentedMap
import cwltool.workflow
from cwltool import load_tool
from cwltool.context import LoadingContext, RuntimeContext
from cwltool.errors import WorkflowException
from cwltool.executors import JobExecutor, SingleJobExecutor
from cwltool.process import Process
from cwltool.utils import CWLObjectType
from cwltool import workflow
from cwltool.context import getdefault
from cwltool.cuda import cuda_version_and_device_count
from cwltool.cwlprov.provenance_profile import ProvenanceProfile
from cwltool.errors import WorkflowException
from cwltool.job import JobBase
from cwltool.loghandler import _logger
from cwltool.mutation import MutationManager
from cwltool.process import Process, cleanIntermediate, relocateOutputs
from cwltool.task_queue import TaskQueue
from cwltool.update import ORIGINAL_CWLVERSION
from cwltool.utils import CWLObjectType, JobsType
from cwltool import command_line_tool
import logging
from typing import Optional, Union
import datetime
import functools
import logging
import math
import os
import threading
from abc import ABCMeta, abstractmethod
from threading import Lock
from typing import (
    Dict,
    Iterable,
    List,
    MutableSequence,
    Optional,
    Set,
    Tuple,
    Union,
    cast,
)

import psutil
from mypy_extensions import mypyc_attr
from schema_salad.exceptions import ValidationException
from schema_salad.sourceline import SourceLine

from cwltool.command_line_tool import CallbackJob, ExpressionJob
from cwltool.context import RuntimeContext, getdefault
from cwltool.cuda import cuda_version_and_device_count
from cwltool.cwlprov.provenance_profile import ProvenanceProfile
from cwltool.errors import WorkflowException
from cwltool.job import JobBase
from cwltool.loghandler import _logger
from cwltool.mutation import MutationManager
from cwltool.process import Process, cleanIntermediate, relocateOutputs
from cwltool.task_queue import TaskQueue
from cwltool.update import ORIGINAL_CWLVERSION
from cwltool.utils import CWLObjectType, JobsType
# from cwltool.workflow import Workflow
from airflow.hara.cwl_tools.hara_engine import cmdlinetool_runner
from airflow.hara.cwl_tools.hara_engine import hara_command_line_tool, hara_workflow, hara_load_tool, hara_job, \
    hara_workflow_job
import pickle
from airflow.hara.cwl_tools.hara_engine import constants


# refer to factory.WorkflowStatus
class HaraWorkflowStatus(Exception):
    def __init__(self, out: Optional[CWLObjectType], status: str) -> None:
        """Signaling exception for the status of a Workflow."""
        super().__init__("Completed %s" % status)
        self.out = out
        self.status = status


# refer to factory.Callable
class HaraCallable:
    """Result of ::py:func:`Factory.make`."""

    def __init__(self, t: Process, factory: "Factory") -> None:
        """Initialize."""
        self.t = t
        self.factory = factory

    def __call__(self, **kwargs):
        # type: (**Any) -> Union[str, Optional[CWLObjectType]]
        runtime_context = self.factory.runtime_context.copy()
        runtime_context.basedir = os.getcwd()
        out, status = self.factory.executor(self.t, kwargs, runtime_context)
        if status != "success":
            raise HaraWorkflowStatus(out, status)
        else:
            return out


# refer to /home/typingliu/.conda/envs/airflow21_dev2/lib/python3.8/site-packages/cwltool/workflow.py default_make_tool()
def hara_make_tool(toolpath_object: CommentedMap, loadingContext: LoadingContext) -> Process:
    """Instantiate the given CWL Process."""
    if not isinstance(toolpath_object, MutableMapping):
        raise WorkflowException("Not a dict: '%s'" % toolpath_object)
    if "class" in toolpath_object:
        if toolpath_object["class"] == "CommandLineTool":
            # return cmdlinetool_runner.HaraLocalCommandLineTool(toolpath_object, loadingContext)
            # return command_line_tool.CommandLineTool(toolpath_object, loadingContext)
            return hara_command_line_tool.HaraCommandLineTool(toolpath_object, loadingContext)
        if toolpath_object["class"] == "Workflow":
            return hara_workflow.HaraWorkflow(toolpath_object, loadingContext)

    raise WorkflowException(
        "Missing or invalid 'class' field in "
        "%s, expecting one of: CommandLineTool, ExpressionTool, Workflow" % toolpath_object["id"]
    )


# refer to factory.Factory
class HaraCwlEngine:
    # static
    h_loading_context: LoadingContext
    h_runtime_context: RuntimeContext

    def __init__(
        self,
        h_executor: Optional[JobExecutor] = None,
        h_loading_context: Optional[LoadingContext] = None,
        h_runtime_context: Optional[RuntimeContext] = None,
    ) -> None:
        # TODO tp: I will init the executor for each step
        # if h_executor is None:
        #     self.h_executor = SingleJobExecutor()
        # else:
        #     self.h_executor = h_executor
        if h_runtime_context is None:
            self.h_runtime_context = RuntimeContext()
        else:
            self.h_runtime_context = h_runtime_context
        if h_loading_context is None:
            self.h_loading_context = LoadingContext()
            self.h_loading_context.singularity = self.h_runtime_context.singularity  # TODO tp: what's it?
            self.h_loading_context.podman = self.h_runtime_context.podman  # TODO tp: what's it?
        else:
            self.h_loading_context = h_loading_context

        self.final_output: MutableSequence[Optional[CWLObjectType]] = []
        self.final_status: List[str] = []
        self.output_dirs: Set[str] = set()

        self.h_loading_context.construct_tool_object = hara_make_tool

    # refer to factory.make
    def load_configuration(self, cwl: str) -> hara_workflow.HaraWorkflow:  # workflow.Workflow is derived from Process
        """Instantiate a CWL object from a CWl document."""
        # workflow_process = load_tool.load_tool(cwl, self.h_loading_context)
        workflow_process = hara_load_tool.load_tool(cwl, self.h_loading_context)
        if isinstance(workflow_process, int):
            raise WorkflowException("Error loading tool")
        # return HaraCallable(load, self)
        return workflow_process

    # refer to /home/typingliu/.conda/envs/airflow21_dev2/lib/python3.8/site-packages/cwltool/executors.py
    # class JobExecutor(metaclass=ABCMeta):     def execute(
    def hara_execute(
        self,
        process: Process,
        job_order_object: CWLObjectType,
        runtime_context: RuntimeContext,
        logger: logging.Logger = _logger,
    ) -> Tuple[Union[Optional[CWLObjectType]], str]:

        self.final_output = []
        self.final_status = []

        if not runtime_context.basedir:
            raise WorkflowException("Must provide 'basedir' in runtimeContext")

        def check_for_abstract_op(tool: CWLObjectType) -> None:
            if tool["class"] == "Operation":
                raise SourceLine(tool, "class", WorkflowException, runtime_context.debug).makeError(
                    "Workflow has unrunnable abstract Operation"
                )

        process.visit(check_for_abstract_op)

        finaloutdir = None  # Type: Optional[str]
        original_outdir = runtime_context.outdir
        if isinstance(original_outdir, str):
            finaloutdir = os.path.abspath(original_outdir)
        runtime_context = runtime_context.copy()
        ## hara TODO: check what does the outdir do in the copied runtime_context. It caused the occurance of a useless folder without being cleaned.
        outdir = runtime_context.create_outdir()
        ## hara end
        self.output_dirs.add(outdir)
        runtime_context.outdir = outdir
        runtime_context.mutation_manager = MutationManager()
        runtime_context.toplevel = True
        runtime_context.workflow_eval_lock = threading.Condition(threading.RLock())

        job_reqs: Optional[List[CWLObjectType]] = None
        if "https://w3id.org/cwl/cwl#requirements" in job_order_object:
            if process.metadata.get(ORIGINAL_CWLVERSION) == "v1.0":
                raise WorkflowException(
                    "`cwl:requirements` in the input object is not part of CWL "
                    "v1.0. You can adjust to use `cwltool:overrides` instead; or you "
                    "can set the cwlVersion to v1.1"
                )
            job_reqs = cast(
                List[CWLObjectType],
                job_order_object["https://w3id.org/cwl/cwl#requirements"],
            )
        elif "cwl:defaults" in process.metadata and "https://w3id.org/cwl/cwl#requirements" in cast(
            CWLObjectType, process.metadata["cwl:defaults"]
        ):
            if process.metadata.get(ORIGINAL_CWLVERSION) == "v1.0":
                raise WorkflowException(
                    "`cwl:requirements` in the input object is not part of CWL "
                    "v1.0. You can adjust to use `cwltool:overrides` instead; or you "
                    "can set the cwlVersion to v1.1"
                )
            job_reqs = cast(
                Optional[List[CWLObjectType]],
                cast(CWLObjectType, process.metadata["cwl:defaults"])[
                    "https://w3id.org/cwl/cwl#requirements"
                ],
            )
        if job_reqs is not None:
            for req in job_reqs:
                process.requirements.append(req)

        # the following is to run a workflow, inside which the workflow is split into multiple steps.

        # hara changed: pass into which step to run
        self.run_jobs(process, job_order_object, logger, runtime_context)
        if runtime_context.validate_only is True:
            return (None, "ValidationSuccess")

        ## hara changed: when running in whole-workflow mode, it should be true, since the last node is a final node.
        if (
            constants.get_hara_context().is_separate_mode and constants.get_hara_context().is_final_step
        ) or (
            not constants.get_hara_context().is_separate_mode
        ):
            ## hara end
            if self.final_output and self.final_output[0] is not None and finaloutdir is not None:
                self.final_output[0] = relocateOutputs(
                    self.final_output[0],
                    finaloutdir,
                    self.output_dirs,
                    runtime_context.move_outputs,
                    runtime_context.make_fs_access(""),
                    getdefault(runtime_context.compute_checksum, True),
                    path_mapper=runtime_context.path_mapper,
                )

            if runtime_context.rm_tmpdir:
                if not runtime_context.cachedir:
                    output_dirs: Iterable[str] = self.output_dirs
                else:
                    output_dirs = filter(
                        lambda x: not x.startswith(runtime_context.cachedir),  # type: ignore
                        self.output_dirs,
                    )
                cleanIntermediate(output_dirs)

            if self.final_output and self.final_status:
                if (
                    runtime_context.research_obj is not None
                    and isinstance(process, (
                    JobBase, Process, hara_workflow_job.WorkflowJobStep, hara_workflow_job.HaraWorkflowJob))
                    and process.parent_wf
                ):
                    process_run_id: Optional[str] = None
                    name = "primary"
                    process.parent_wf.generate_output_prov(self.final_output[0], process_run_id, name)
                    process.parent_wf.document.wasEndedBy(
                        process.parent_wf.workflow_run_uri,
                        None,
                        process.parent_wf.engine_uuid,
                        datetime.datetime.now(),
                    )
                    process.parent_wf.finalize_prov_profile(name=None)
                return (self.final_output[0], self.final_status[0])
            return (None, "permanentFail")
        else:
            return (None, 'success')

    # refer to /home/typingliu/.conda/envs/airflow21_dev2/lib/python3.8/site-packages/cwltool/executors.py
    # class JobExecutor(metaclass=ABCMeta):
    #     def output_callback(self, out: Optional[CWLObjectType], process_status: str) -> None:
    def output_callback(self, out: Optional[CWLObjectType], process_status: str) -> None:
        """Collect the final status and outputs."""
        self.final_status.append(process_status)
        self.final_output.append(out)

    # refer to /home/typingliu/.conda/envs/airflow21_dev2/lib/python3.8/site-packages/cwltool/executors.py
    # class SingleJobExecutor(JobExecutor): run_jobs
    def run_jobs(
        self,
        process: Process,
        job_order_object: CWLObjectType,
        logger: logging.Logger,
        runtime_context: RuntimeContext,
    ) -> None:
        # process_run_id: Optional[str] = None

        # define provenance profile for single commandline tool
        if not isinstance(process, hara_workflow.HaraWorkflow) and runtime_context.research_obj is not None:
            process.provenance_object = ProvenanceProfile(
                runtime_context.research_obj,
                full_name=runtime_context.cwl_full_name,
                host_provenance=False,
                user_provenance=False,
                orcid=runtime_context.orcid,
                # single tool execution, so RO UUID = wf UUID = tool UUID
                run_uuid=runtime_context.research_obj.ro_uuid,
                fsaccess=runtime_context.make_fs_access(""),
            )
            process.parent_wf = process.provenance_object

        ## hara changed
        # jobiter = process.job(job_order_object, self.output_callback, runtime_context)
        jobiter = process.job(job_order_object, self.output_callback, runtime_context)
        ## hara end
        # cwltool.workflow.Workflow
        try:
            for job in jobiter:
                if job is not None:
                    if runtime_context.builder is not None and hasattr(job, "builder"):
                        job.builder = runtime_context.builder
                    if job.outdir is not None:
                        self.output_dirs.add(job.outdir)
                        logger.info("job tmp outdir:" + job.outdir)
                    ## hara changed: the code seems not important
                    # if runtime_context.research_obj is not None:
                    #     if not isinstance(process, Workflow):
                    #         prov_obj = process.provenance_object
                    #     else:
                    #         prov_obj = job.prov_obj
                    #     if prov_obj:
                    #         runtime_context.prov_obj = prov_obj
                    #         prov_obj.fsaccess = runtime_context.make_fs_access("")
                    #         prov_obj.evaluate(
                    #             process,
                    #             job,
                    #             job_order_object,
                    #             runtime_context.research_obj,
                    #         )
                    #         process_run_id = prov_obj.record_process_start(process, job)
                    #         runtime_context = runtime_context.copy()
                    #     runtime_context.process_run_id = process_run_id
                    # if runtime_context.validate_only is True:
                    #     if isinstance(job, WorkflowJob):
                    #         name = job.tool.lc.filename
                    #     else:
                    #         name = getattr(job, "name", str(job))
                    #     print(
                    #         f"{name} is valid CWL. No errors detected in the inputs.",
                    #         file=runtime_context.validate_stdout,
                    #     )
                    #     return

                    job.run(runtime_context)
                else:
                    logger.error("Workflow cannot make any more progress.")
                    break
        except (
            ValidationException,
            WorkflowException,
        ):  # pylint: disable=try-except-raise
            raise
        except Exception as err:
            logger.exception("Got workflow error")
            raise WorkflowException(str(err)) from err


# job can be WorkflowJob and CommandLineJob
def serialize_job(job, runtime_context: RuntimeContext):
    pass;
