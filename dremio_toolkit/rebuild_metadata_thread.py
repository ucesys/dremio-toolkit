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

import threading

from dremio_toolkit.logger import Logger
from dremio_toolkit.env_api import EnvApi
from dremio_toolkit.context import Context


class RebuildMetadataThread(threading.Thread):

    def __init__(self, context: Context, pds_path: str, refresh_only: False):
        threading.Thread.__init__(self)
        self._context = context
        self._logger = context.get_logger()
        self._env_api = context.get_target_env_api()
        self._pds_path = pds_path
        self._status = None
        self._forget_job_id = None
        self._refresh_job_id = None
        self._forget_job_info = None
        self._refresh_job_info = None
        self._refresh_only = refresh_only

    def run(self):
        if not self._refresh_only:
            success, jobid, job_info = self._env_api.execute_sql(self._context.get_sql_comment_uuid() +
                                                                 'ALTER PDS ' + self._pds_path + ' FORGET METADATA')
            self._forget_job_id = jobid
            self._forget_job_info = job_info
            if not success:
                self._logger.error('Unable to ALTER PDS: ' + str(self._pds_path) + ' jobid: ' + str(jobid) + ' jobInfo: ' + str(job_info))
                self._status = success
                self._logger.print_process_status(increment=1)
                return

        success, jobid, job_info = self._env_api.execute_sql(self._context.get_sql_comment_uuid() +
                                            'ALTER PDS ' + self._pds_path + ' REFRESH METADATA AUTO PROMOTION')
        self._refresh_job_id = jobid
        self._refresh_job_info = job_info
        self._status = success
        self._logger.print_process_status(increment=1)

    def get_forget_job_id(self):
        if self._refresh_only:
            return 'N/A - refresh only run'
        return self._forget_job_id

    def get_refresh_job_id(self):
        return self._refresh_job_id

    def get_pds_path(self):
        return self._pds_path

    def get_status(self):
        return self._status

    def get_forget_job_info(self):
        return self._forget_job_info

    def get_refresh_job_info(self):
        return self._refresh_job_info
