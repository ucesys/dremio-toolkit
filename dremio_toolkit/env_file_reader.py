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
import os
from dremio_toolkit.env_definition import EnvDefinition
from dremio_toolkit.env_file_writer import EnvFileWriter


###
# This class uses DremioData object to update Dremio environment.
###
class EnvFileReader:

    @staticmethod
    def read_dremio_environment_from_file(filename: str):
        f = open(filename, "r", encoding="utf-8")
        data = json.load(f)['data']
        f.close()
        env_def = EnvDefinition()
        for item in data:
            if 'dremio_environment' in item:
                for env_item in item['dremio_environment']:
                    if 'endpoint' in env_item:
                        env_def.endpoint = env_item['endpoint']
                        break
            elif 'containers' in item:
                env_def.containers = item['containers']
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

    @staticmethod
    def read_dremio_environment_from_directory(source_directory):
        try:
            env_def = EnvDefinition()
            f = open(os.path.join(source_directory, EnvFileWriter.DREMIO_ENV_FILENAME), "r", encoding="utf-8")
            env_def.dremio_get_config = json.load(f)
            f.close()
            EnvFileReader._collect_directory(os.path.join(source_directory, 'sources'), env_def.sources, None, None)
            EnvFileReader._collect_directory(os.path.join(source_directory, 'spaces'), env_def.spaces, env_def.folders,
                                             env_def.vds_list)
            EnvFileReader._collect_directory(os.path.join(source_directory, 'reflections'), None, None,
                                             env_def.reflections)
            EnvFileReader._collect_directory(os.path.join(source_directory, 'rules'), None, None, env_def.rules)
            EnvFileReader._collect_directory(os.path.join(source_directory, 'queues'), None, None, env_def.queues)
            EnvFileReader._collect_directory(os.path.join(source_directory, 'tags'), None, None, env_def.tags)
            EnvFileReader._collect_directory(os.path.join(source_directory, 'wikis'), None, None, env_def.wikis)
            EnvFileReader._collect_directory(os.path.join(source_directory, 'referenced_users'), None, None,
                                             env_def.referenced_users)
            EnvFileReader._collect_directory(os.path.join(source_directory, 'referenced_groups'), None, None,
                                             env_def.referenced_groups)
            EnvFileReader._collect_directory(os.path.join(source_directory, 'referenced_roles'), None, None,
                                             env_def.referenced_roles)
            EnvFileReader._collect_directory(os.path.join(source_directory, 'vds_parents'), None, None,
                                             env_def.vds_parents)
        except OSError as e:
            raise Exception("Error reading file. OS Error: " + e.strerror)
        return env_def

    @staticmethod
    def _collect_directory(directory, container_list, folder_list, object_list):
        for (dirpath, dirnames, filenames) in os.walk(directory):
            for filename in filenames:
                f = open(os.path.join(dirpath, filename), "r", encoding="utf-8")
                data = json.load(f)
                f.close()
                if EnvFileWriter.CONTAINER_SELF_FILENAME == filename:
                    # First level of dirpath is a container if container_list passed
                    if container_list is None or (
                            '/' in dirpath[len(directory) + 1:] or '\\' in dirpath[len(directory) + 1:]):
                        if folder_list is not None:
                            folder_list.append(data)
                    else:
                        container_list.append(data)
                else:
                    if object_list is not None:
                        object_list.append(data)
