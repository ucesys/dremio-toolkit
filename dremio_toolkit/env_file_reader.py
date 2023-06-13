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
from dremio_toolkit.context import Context
from dremio_toolkit.env_definition import EnvDefinition

###
# This class uses DremioData object to update Dremio environment.
###
class EnvFileReader:

    DREMIO_ENV_FILE_VERSION_20 = '2.0'

    @staticmethod
    def read_dremio_source_environment(context: Context):
        if context.get_input_mode() == Context.PATH_MODE_FILE:
            return EnvFileReader._read_dremio_environment_from_file(context, context.get_input_path())
        else:
            return EnvFileReader._read_dremio_environment_from_directory(context, context.get_input_path())

    @staticmethod
    def read_dremio_target_environment(context: Context):
        if context.get_output_mode() == Context.PATH_MODE_FILE:
            return EnvFileReader._read_dremio_environment_from_file(context, context.get_output_path())
        else:
            return EnvFileReader._read_dremio_environment_from_directory(context, context.get_output_path())

    @staticmethod
    def _read_dremio_environment_from_file(context: Context, filename: str):
        f = open(filename, "r", encoding="utf-8")
        data = json.load(f)['data']
        f.close()
        env_def = EnvDefinition()
        if 'dremio_environment' in data:
            for env_item in data['dremio_environment']:
                if 'endpoint' in env_item:
                    env_def.endpoint = env_item['endpoint']
                elif 'file_version' in env_item:
                    env_def.file_version = env_item['file_version']
                elif 'timestamp_utc' in env_item:
                    env_def.timestamp_utc = env_item['timestamp_utc']
        if env_def.file_version == EnvFileReader.DREMIO_ENV_FILE_VERSION_20:
            return EnvFileReader._read_dremio_environment_from_file_fv20(data, env_def)
        else:
            context.get_logger().fatal("Unsupported file version: " + str(env_def.file_version))

    @staticmethod
    def _read_dremio_environment_from_file_fv20(data, env_def: EnvDefinition):
        if 'sources' in data:
            env_def.sources = data['sources']
        if 'spaces' in data:
            env_def.spaces = data['spaces']
        if 'folders' in data:
            env_def.folders = data['folders']
        if 'vds' in data:
            env_def.vds_list = data['vds']
        if 'vds_parents' in data:
            env_def.vds_parents = data['vds_parents']
        if 'reflections' in data:
            env_def.reflections = data['reflections']
        if 'referenced_users' in data:
            env_def.referenced_users = data['referenced_users']
        if 'referenced_groups' in data:
            env_def.referenced_groups = data['referenced_groups']
        if 'referenced_roles' in data:
            env_def.referenced_roles = data['referenced_roles']
        if 'queues' in data:
            env_def.queues = data['queues']
        if 'rules' in data:
            env_def.rules = data['rules']
        if 'tags' in data:
            env_def.tags = data['tags']
        if 'wikis' in data:
            env_def.wikis = data['wikis']
        if 'votes' in data:
            env_def.votes = data['votes']
        if 'vds_parents' in data:
            env_def.vds_parents = data['vds_parents']
        return env_def

    @staticmethod
    def _read_dremio_environment_from_directory(context: Context, source_directory: str):
        try:
            env_def = EnvDefinition()
            f = open(os.path.join(source_directory, EnvFileWriter.DREMIO_ENV_FILENAME), "r", encoding="utf-8")
            dremio_environment = json.load(f)['dremio_environment']
            for env_item in dremio_environment:
                if 'endpoint' in env_item:
                    env_def.endpoint = env_item['endpoint']
                elif 'file_version' in env_item:
                    env_def.file_version = env_item['file_version']
                elif 'timestamp_utc' in env_item:
                    env_def.timestamp_utc = env_item['timestamp_utc']
            f.close()
            EnvFileReader._collect_directory(os.path.join(source_directory, 'containers'), env_def.containers, None, None)
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
