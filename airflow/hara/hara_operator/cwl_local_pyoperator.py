#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

from typing import Any, Mapping

from airflow.hara.cwl_tools import hara_cwl_entry
from airflow.hara.cwl_tools.tools import cwl_log
from airflow.hara.common_tools import path_tools
from airflow.hara.cwl_tools.hara_engine import controller
from airflow.models.baseoperator import BaseOperator
from airflow.utils.operator_helpers import KeywordParameters
from airflow.utils.context import Context
import os


class CwlLocalOperator(BaseOperator):
    """
    Executes a Python callable

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:PythonOperator`

    When running your callable, Airflow will pass a set of keyword arguments that can be used in your
    function. This set of kwargs correspond exactly to what you can use in your jinja templates.
    For this to work, you need to define ``**kwargs`` in your function header, or you can add directly the
    keyword arguments you would like to get - for example with the below code your callable will get
    the values of ``ti`` and ``next_ds`` context variables.

    With explicit arguments:

    .. code-block:: python

       def my_python_callable(ti, next_ds):
           pass

    With kwargs:

    .. code-block:: python

       def my_python_callable(**kwargs):
           ti = kwargs["ti"]
           next_ds = kwargs["next_ds"]


    :param python_callable: A reference to an object that is callable
    :param op_kwargs: a dictionary of keyword arguments that will get unpacked
        in your function
    :param op_args: a list of positional arguments that will get unpacked when
        calling your callable
    :param templates_dict: a dictionary where the values are templates that
        will get templated by the Airflow engine sometime between
        ``__init__`` and ``execute`` takes place and are made available
        in your callable's context after the template has been applied. (templated)
    :param templates_exts: a list of file extensions to resolve while
        processing templated fields, for examples ``['.sql', '.hql']``
    :param show_return_value_in_logs: a bool value whether to show return_value
        logs. Defaults to True, which allows return value log output.
        It can be set to False to prevent log output of return value when you return huge data
        such as transmission a large amount of XCom to TaskAPI.
    """

    def __init__(
        self,
        *,
        main_cwl_file_path: str,
        cwl_step_to_run: str,
        basedir: str,
        job_file_path: str,
        cwl_work_path: str,
        job_content: dict,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.main_cwl_file_path = main_cwl_file_path
        self.cwl_step_to_run = cwl_step_to_run
        self.basedir = basedir
        self.job_file_path = job_file_path
        self.cwl_work_path = cwl_work_path
        self.job_content = job_content

    def execute(self, context: Context) -> Any:
        cwl_log.get_cwl_logger().info("start executing")

        job_content = context['dag_run'].conf.get("job_content")
        if job_content is None:
            job_content = self.job_content

        if job_content is not None:
            cwl_log.get_cwl_logger().info("-----job_content: %s", job_content) # {'message_for_step1': 'Kyoto Osaka Fukuoka Osaka Nagoya Hokkaido'}
            cwl_log.get_cwl_logger().info("-----job_content type: %s", type(job_content)) # dict

        run_id = context['dag_run'].run_id
        # path_safe_run_id = run_id.replace(':', '_').replace(' ', '_').replace('+', '_')
        path_safe_run_id = path_tools.convert_to_valid_filename(run_id)

        main_cwl_file_path = self.main_cwl_file_path
        job_file_path = self.job_file_path

        tmpdir_prefix = os.path.join(self.cwl_work_path, path_safe_run_id, 'tmp_outdir')
        tmp_outdir_prefix = os.path.join(self.cwl_work_path, path_safe_run_id)
        # runtime_context.tmpdir = '/home/typingliu/temp/tmpdir/'
        stagedir = os.path.join(self.cwl_work_path, path_safe_run_id, 'stagedir/')
        outdir = os.path.join(self.cwl_work_path, path_safe_run_id, 'outdir/')

        basedir = self.basedir
        file_kv_path = os.path.join(self.cwl_work_path, path_safe_run_id, 'hara_kv_db.json')
        cwl_step_to_run = self.cwl_step_to_run

        hara_cwl_engine = controller.HaraCwlEngine()
        workflow_process = hara_cwl_engine.load_configuration(main_cwl_file_path)


        # TODO hara: host network


        cwl_log.get_cwl_logger().info("-----execute cwl with the these parameters")
        cwl_log.get_cwl_logger().info("main_cwl_file_path: %s", main_cwl_file_path)
        return_value = hara_cwl_entry.execute_cwl(hara_cwl_engine.h_runtime_context, job_file_path,
                                                  workflow_process=workflow_process,
                                                  tmpdir_prefix=tmpdir_prefix,
                                                  tmp_outdir_prefix=tmp_outdir_prefix,
                                                  stagedir=stagedir,
                                                  outdir=outdir,
                                                  basedir=basedir,
                                                  run_id=path_safe_run_id,
                                                  file_kv_path=file_kv_path,
                                                  step_to_run=cwl_step_to_run,
                                                  is_separate_mode=True,
                                                  job_content=job_content,
                                                  )

        cwl_log.get_cwl_logger().info(return_value)
        return return_value

    def determine_kwargs(self, context: Mapping[str, Any]) -> Mapping[str, Any]:
        return KeywordParameters.determine(self.python_callable, self.op_args, context).unpacking()

    def execute_callable(self) -> Any:
        """
        Calls the python callable with the given arguments.

        :return: the return value of the call.
        """
        return self.python_callable(*self.op_args, **self.op_kwargs)
