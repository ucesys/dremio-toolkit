#########################################################################
# Copyright (C) 2023 UCE Systems Corporation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Contact dremio@ucesys.com
#########################################################################

from uuid import uuid4
from datetime import datetime
from dremio_toolkit.logger import Logger
from dremio_toolkit.env_api import EnvApi


class Context:
    FATAL_EXIT_CODE = 1
    NON_FATAL_EXIT_CODE = 2

    CMD_NOT_SPECIFIED = 'cmd_not_specified'
    CMD_CREATE_SNAPSHOT = 'create_snapshot'
    CMD_PUSH_SNAPSHOT = 'push_snapshot'
    CMD_EXPLODE_SNAPSHOT = 'explode_snapshot'
    CMD_IMPLODE_SNAPSHOT = 'explode_snapshot'
    CMD_DIFF_SNAPSHOT = 'diff_snapshot'
    CMD_EXEC_SQL = 'exec_sql'
    CMD_REBUILD_METADATA = 'rebuild_metadata'

    PATH_MODE_FILE = 'FILE'
    PATH_MODE_DIR = 'DIR'

    def __init__(self, command: str = None):
        if command is None:
            self._command = Context.CMD_NOT_SPECIFIED
        else:
            self._command = command
        self._uuid = str(uuid4())

        self._logger = None

        self._env_api_source = None
        self._env_api_target = None

        self._input_mode = None
        self._input_path = None
        self._output_mode = None
        self._output_path = None

        self._report_filepath = None
        self._report_delimiter = None

        return

    def get_command(self):
        return self._command

    def init_logger(self, log_level: str, log_verbose: bool, log_filepath: str = None):
        self._logger = Logger(self, level=log_level, verbose=log_verbose, log_file=log_filepath)
        return self._logger

    def set_logger(self, logger: Logger):
        self._logger = logger
        self._logger.set_uuid(self._uuid)

    def get_logger(self):
        return self._logger

    def get_uuid(self):
        return self._uuid

    def get_sql_comment_uuid(self):
        return '// dremio-toolkit \n// run_id: ' + self.get_uuid() + '\n'

    def set_source_env_api(self, env_api: EnvApi):
        self._env_api_source = env_api

    def get_source_env_api(self):
        return self._env_api_source

    def get_source_env_version(self):
        return self._env_api_source.get_dremio_version()

    def set_target_env_api(self, env_api: EnvApi):
        self._env_api_source = env_api

    def get_target_env_api(self):
        return self._env_api_target

    def get_target_env_version(self):
        return self._env_api_target.get_dremio_version()

    def set_source(self, env_api: EnvApi = None, input_mode: str = None, input_path: str = None):
        self._env_api_source = env_api
        self._input_mode = input_mode
        self._input_path = input_path

    def get_input_mode(self):
        return self._input_mode

    def get_input_path(self):
        return self._input_path

    def set_target(self, env_api: EnvApi = None, output_mode: str = None, output_path: str = None):
        self._env_api_target = env_api
        self._output_mode = output_mode
        self._output_path = output_path

    def get_output_mode(self):
        return self._output_mode

    def get_output_path(self):
        return self._output_path

    def set_report(self, report_filepath: str = None, report_delimiter: str = None):
        self._report_filepath = report_filepath
        self._report_delimiter = report_delimiter

    def get_report_filepath(self):
        if self._report_filepath is None:
            return self._command + '_report_' + str(datetime.now()) + ".txt"
        return self._report_filepath

    def get_report_delimiter(self):
        return self._report_delimiter
