import copy
import datetime
import functools
import logging
import random
import ruamel
from typing import (
    Callable,
    Dict,
    Iterable,
    List,
    Mapping,
    MutableMapping,
    MutableSequence,
    Optional,
    Union,
    cast,
)
from uuid import UUID

from mypy_extensions import mypyc_attr
from ruamel.yaml.comments import CommentedMap
from schema_salad.exceptions import ValidationException
from schema_salad.sourceline import SourceLine, indent

from cwltool import workflow
from cwltool import command_line_tool, context, procgenerator
from cwltool.checker import circular_dependency_checker, loop_checker, static_checker
from cwltool.context import LoadingContext, RuntimeContext, getdefault
from cwltool.cwlprov.provenance_profile import ProvenanceProfile
from cwltool.cwlprov.writablebagfile import create_job
from cwltool.errors import WorkflowException
from cwltool.load_tool import load_tool
from cwltool.loghandler import _logger
from cwltool.process import Process, get_overrides, shortname
from cwltool.utils import (
    CWLObjectType,
    CWLOutputType,
    JobsGeneratorType,
    OutputCallbackType,
    StepType,
    aslist,
)

# /home/typingliu/.conda/envs/airflow21_dev2/lib/python3.8/site-packages/cwltool/workflow.py WorkflowStep(Process).__init__()
# def init_hara_commandlinetool(
#     # toolpath_object: CommentedMap,
#     commandline_file_path: str, # 'file:///home/typingliu/workspace/tpy/airflow21/airflow/airflow/hara_bin/cwl_tools/rw_example/hara_write.cwl.yaml'
#     # pos: int, # the order os step
#     loadingContext: LoadingContext,
#     workflow_step_in_field : ruamel.yaml.comments.CommentedSeq, # toolpath_object[stepfield]: [{'source': 'file:///home/typingliu/workspace/tpy/airflow21/airflow/airflow/hara_bin/cwl_tools/rw_example/hara_workflow.cwl.yaml#message_for_step1', 'id': 'file:///home/typingliu/workspace/tpy/airflow21/airflow/airflow/hara_bin/cwl_tools/rw_example/hara_workflow.cwl.yaml#writeMessage/sentence_to_wc'}]
#     workflow_step_out_field: ruamel.yaml.comments.CommentedSeq, # toolpath_object[stepfield]
# ) -> Process:
#     hara_commandlinetool = load_tool(commandline_file_path, loadingContext)
#
#     toolpath_object = {}
#     toolpath_object['in'] = workflow_step_in_field
#     toolpath_object['out'] = workflow_step_out_field
#
#     validation_errors = []
#     bound = set()
#     for stepfield, toolfield in (("in", "inputs"), ("out", "outputs")):
#         toolpath_object[toolfield] = []
#         for index, step_entry in enumerate(toolpath_object[stepfield]):
#             if isinstance(step_entry, str):
#                 param: CommentedMap = CommentedMap()
#                 inputid = step_entry
#             else:
#                 param = CommentedMap(step_entry.items())
#                 inputid = step_entry["id"]
#
#             shortinputid = shortname(inputid)
#             found = False
#             for tool_entry in hara_commandlinetool.tool[toolfield]: # # [{'type': 'string', 'default': 'Hello Harada', 'inputBinding': {'position': 1}, 'id': 'file:///home/typingliu/workspace/tpy/airflow21/airflow/airflow/hara_bin/cwl_tools/rw_example/hara_write.cwl.yaml#sentence_to_wc'}]
#                 frag = shortname(tool_entry["id"])
#                 if frag == shortinputid:
#                     # if the case that the step has a default for a parameter,
#                     # we do not want the default of the tool to override it
#                     step_default = None
#                     if "default" in param and "default" in tool_entry:
#                         step_default = param["default"]
#                     param.update(tool_entry)
#                     param["_tool_entry"] = tool_entry
#                     if step_default is not None:
#                         param["default"] = step_default
#                     found = True
#                     bound.add(frag)
#                     break
#             if not found:
#                 if stepfield == "in":
#                     param["type"] = "Any"
#                     param["used_by_step"] = workflow.used_by_step(hara_commandlinetool.tool, shortinputid)
#                     param["not_connected"] = True
#                 else:
#                     if isinstance(step_entry, Mapping):
#                         step_entry_name = step_entry["id"]
#                     else:
#                         step_entry_name = step_entry
#                     validation_errors.append("error")
#             param["id"] = inputid
#             param.lc.line = toolpath_object[stepfield].lc.data[index][0]
#             param.lc.col = toolpath_object[stepfield].lc.data[index][1]
#             param.lc.filename = toolpath_object[stepfield].lc.filename
#             toolpath_object[toolfield].append(param)
#
#         missing_values = []
#         for _, tool_entry in enumerate(hara_commandlinetool.tool["inputs"]):
#             if shortname(tool_entry["id"]) not in bound:
#                 if "null" not in tool_entry["type"] and "default" not in tool_entry:
#                     missing_values.append(shortname(tool_entry["id"]))
