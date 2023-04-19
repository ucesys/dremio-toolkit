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

from dremio_toolkit.logger import Logger
from dremio_toolkit.env_api import EnvApi
import threading


class RebuildMetadataThread(threading.Thread):

    def __init__(self, env_api: EnvApi, pds_path, logger: Logger):
        threading.Thread.__init__(self)
        self._logger = logger
        self._env_api = env_api
        self._pds_path = pds_path
        self._status = None
        self._forget_job_info = None
        self._refresh_job_info = None

    def run(self):
        success, jobid, job_info = self._env_api.execute_sql('ALTER PDS ' + self._pds_path + ' FORGET METADATA')
        self._forget_job_info = job_info
        if success:
            success, jobid, job_info = self._env_api.execute_sql('ALTER PDS ' + self._pds_path + ' REFRESH METADATA AUTO PROMOTION')
            self._refresh_job_info = job_info
        if not success:
            self._logger.error('Unable to ALTER PDS. ' + str(job_info))
        self._status = success
        self._logger.print_process_status(increment=1)

    def get_pds_path(self):
        return self._pds_path

    def get_status(self):
        return self._status

    def get_forget_job_info(self):
        return self._forget_job_info

    def get_refresh_job_info(self):
        return self._refresh_job_info
