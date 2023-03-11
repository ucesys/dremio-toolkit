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

import json

from dremio_toolkit.env_definition import EnvDefinition


###
# This class uses DremioData object to update Dremio environment.
###
class EnvFileReader:
    @staticmethod
    def read_dremio_environment(filename: str):
        f = open(filename, "r", encoding="utf-8")
        data = json.load(f)['data']
        f.close()
        env_def = EnvDefinition()
        for item in data:
            if 'dremio_environment' in item:
                for env_item in item['dremio_environment']:
                    if 'endpoint' in env_item:
                        env_def.endpoint = env_item['endpoint']
            elif 'containers' in item:
                env_def.containers = item['containers']
            elif 'homes' in item:
                env_def.homes = item['homes']
            elif 'sources' in item:
                env_def.sources = item['sources']
            elif 'spaces' in item:
                env_def.spaces = item['spaces']
            elif 'folders' in item:
                env_def.folders = item['folders']
            elif 'vds' in item:
                env_def.vds_list = item['vds']
            elif 'vds_parents' in item:
                env_def.vds_parents = item['vds_parents']
            elif 'reflections' in item:
                env_def.reflections = item['reflections']
            elif 'referenced_users' in item:
                env_def.referenced_users = item['referenced_users']
            elif 'referenced_groups' in item:
                env_def.referenced_groups = item['referenced_groups']
            elif 'referenced_roles' in item:
                env_def.referenced_roles = item['referenced_roles']
            elif 'queues' in item:
                env_def.queues = item['queues']
            elif 'rules' in item:
                env_def.rules = item['rules']
            elif 'tags' in item:
                env_def.tags = item['tags']
            elif 'wikis' in item:
                env_def.wikis = item['wikis']
            elif 'votes' in item:
                env_def.votes = item['votes']
            elif 'vds_parents' in item:
                env_def.vds_parents = item['vds_parents']
            elif 'pds' in item:
                pass
            elif 'files' in item:
                pass
            elif 'vds_parents' in item:
                pass
            elif 'dremio_get_config' in item:
                pass
            else:
                pass
        return env_def
