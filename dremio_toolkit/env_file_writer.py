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
import os, errno
from datetime import datetime
from shutil import rmtree


from dremio_toolkit.env_definition import EnvDefinition


class EnvFileWriter:
    CONTAINER_SELF_FILENAME = '___self.json'

    @staticmethod
    def save_dremio_environment_as_file(env_def: EnvDefinition, output_file: str, logger) -> None:
        if os.path.isfile(output_file):
            os.remove(output_file)

        env_snapshot = {
            "data": [
                {'dremio_environment': [
                    {'file_version': '1.0'},
                    {"endpoint": env_def.endpoint},
                    {'timestamp_utc': str(datetime.utcnow())}
                ]},
                {'containers': env_def.containers},
                {'sources': env_def.sources},
                {'spaces': env_def.spaces},
                {'folders': env_def.folders},
                {'vds': env_def.vds_list},
                {'vds_parents': env_def.vds_parents},
                {'files': env_def.files},
                {'reflections': env_def.reflections},
                {'queues': env_def.queues},
                {'rules': env_def.rules},
                {'tags': env_def.tags},
                {'wikis': env_def.wikis},
                {'votes': env_def.votes},
                {'referenced_users': env_def.referenced_users},
                {'referenced_groups': env_def.referenced_groups},
                {'referenced_roles': env_def.referenced_roles},
            ]
        }

        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(env_snapshot, f, indent=4, sort_keys=True)

    @staticmethod
    def save_dremio_environment_as_dir(env_def: EnvDefinition, output_dir: str, logger) -> None:
        try:
            # create directory structure as needed
            if os.path.isdir(output_dir):
                rmtree(output_dir)
            os.makedirs(output_dir)
            os.makedirs(os.path.join(output_dir, 'sources').encode(encoding='utf-8', errors='strict'))
            os.makedirs(os.path.join(output_dir, 'spaces').encode(encoding='utf-8', errors='strict'))
            os.makedirs(os.path.join(output_dir, 'reflections').encode(encoding='utf-8', errors='strict'))
            os.makedirs(os.path.join(output_dir, 'referenced_users').encode(encoding='utf-8', errors='strict'))
            os.makedirs(os.path.join(output_dir, 'referenced_groups').encode(encoding='utf-8', errors='strict'))
            os.makedirs(os.path.join(output_dir, 'referenced_roles').encode(encoding='utf-8', errors='strict'))
            os.makedirs(os.path.join(output_dir, 'queues').encode(encoding='utf-8', errors='strict'))
            os.makedirs(os.path.join(output_dir, 'rules').encode(encoding='utf-8', errors='strict'))
            os.makedirs(os.path.join(output_dir, 'tags').encode(encoding='utf-8', errors='strict'))
            os.makedirs(os.path.join(output_dir, 'wikis').encode(encoding='utf-8', errors='strict'))
            os.makedirs(os.path.join(output_dir, 'votes').encode(encoding='utf-8', errors='strict'))
            os.makedirs(os.path.join(output_dir, 'vds_parents').encode(encoding='utf-8', errors='strict'))
        except OSError as e:
            raise logger.fatal("Error processing directory structure. OS Error: " + e.strerror)
        try:
            f = open(os.path.join(output_dir, 'dremio_environment.json'), "w", encoding="utf-8")
            json.dump({'dremio_environment': [
                            {'file_version': '1.0'},
                            {"endpoint": env_def.endpoint},
                            {'timestamp_utc': str(datetime.utcnow())}
                        ]}, f, indent=4, sort_keys=True)
            f.close()
            for source in env_def.sources:
                os.makedirs(
                    os.path.join(output_dir, "sources", EnvFileWriter._replace_special_characters(source['name'])).
                    encode(encoding='utf-8', errors='strict'))
                EnvFileWriter._write_container_json_file(os.path.join(output_dir, "sources"), source)
            for space in env_def.spaces:
                os.makedirs(
                    os.path.join(output_dir, "spaces", EnvFileWriter._replace_special_characters(space['name'])).
                    encode(encoding='utf-8', errors='strict'))
                EnvFileWriter._write_container_json_file(os.path.join(output_dir, "spaces"), space)
            for folder in env_def.folders:
                # ignore directory exists error, we might have created it prior
                dir_path = os.path.join(output_dir, "spaces", EnvFileWriter._get_fs_path(folder['path'])).\
                    encode(encoding='utf-8', errors='strict')
                if not os.path.isdir(dir_path):
                    os.makedirs(dir_path)
                EnvFileWriter._write_folder_json_file(os.path.join(output_dir, "spaces"), folder)
            for vds in env_def.vds_list:
                EnvFileWriter._write_entity_json_file(os.path.join(output_dir, "spaces"), vds)
            for vds in env_def.vds_parents:
                EnvFileWriter._write_object_json_file(os.path.join(output_dir, "vds_parents"), vds)
            for reflection in env_def.reflections:
                EnvFileWriter._write_object_json_file(os.path.join(output_dir, "reflections"), reflection)
            for rule in env_def.rules:
                EnvFileWriter._write_object_json_file(os.path.join(output_dir, "rules"), rule)
            for queue in env_def.queues:
                EnvFileWriter._write_object_json_file(os.path.join(output_dir, "queues"), queue)
            for vote in env_def.votes:
                EnvFileWriter._write_vote_json_file(os.path.join(output_dir, "votes"), vote)
            for tag in env_def.tags:
                EnvFileWriter._write_tag_json_file(os.path.join(output_dir, "tags"), tag)
            for wiki in env_def.wikis:
                EnvFileWriter._write_wiki_json_file(os.path.join(output_dir, "wikis"), wiki)
            for user in env_def.referenced_users:
                EnvFileWriter._write_object_json_file(os.path.join(output_dir, "referenced_users"), user)
            for group in env_def.referenced_groups:
                EnvFileWriter._write_object_json_file(os.path.join(output_dir, "referenced_groups"), group)
            for role in env_def.referenced_roles:
                EnvFileWriter._write_object_json_file(os.path.join(output_dir, "referenced_roles"), role)
        except OSError as e:
            raise Exception("Error writing file. OS Error: " + e.strerror)

    @staticmethod
    def _write_container_json_file(root_dir, container):
        filepath = os.path.join(root_dir, container['name'],
                                EnvFileWriter.CONTAINER_SELF_FILENAME).encode(encoding='utf-8', errors='strict')
        f = open(filepath, "w", encoding="utf-8")
        json.dump(container, f, indent=4, sort_keys=True)
        f.close()

    @staticmethod
    def _write_folder_json_file(root_dir, folder):
        filepath = os.path.join(root_dir, EnvFileWriter._get_fs_path(folder['path']),
                                EnvFileWriter.CONTAINER_SELF_FILENAME).encode(encoding='utf-8', errors='strict')
        f = open(filepath, "w", encoding="utf-8")
        json.dump(folder, f, indent=4, sort_keys=True)
        f.close()

    @staticmethod
    def _write_entity_json_file(root_dir, entity, postfix=''):
        # check if any folder needs to be created
        path = entity['path']
        filepath = root_dir
        for item in path:
            if not os.path.isdir(filepath.encode(encoding='utf-8', errors='strict')):
                try:
                    os.makedirs(filepath.encode(encoding='utf-8', errors='strict'))
                except OSError as e:
                    if e.errno != errno.EEXIST:
                        raise "Cannot create directory: " + filepath
            # Replace special characters with underscore
            filepath = os.path.join(filepath, EnvFileWriter._replace_special_characters(item))
        # write entity into json file
        filepath = (filepath + postfix + ".json").encode(encoding='utf-8', errors='strict')
        f = open(filepath, "w", encoding="utf-8")
        json.dump(entity, f, indent=4, sort_keys=True)
        f.close()

    @staticmethod
    def _write_object_json_file(root_dir, obj):
        filepath = os.path.join(root_dir, obj['id'] + ".json").encode(encoding='utf-8', errors='strict')
        f = open(filepath, "w", encoding="utf-8")
        json.dump(obj, f, indent=4, sort_keys=True)
        f.close()

    @staticmethod
    def _write_vote_json_file(root_dir, obj):
        filepath = os.path.join(root_dir, obj['datasetId'] + ".json").encode(encoding='utf-8', errors='strict')
        f = open(filepath, "w", encoding="utf-8")
        json.dump(obj, f, indent=4, sort_keys=True)
        f.close()

    @staticmethod
    def _write_tag_json_file(root_dir, tags):
        filepath = os.path.join(root_dir, tags['entity_id'] + ".json").encode(encoding='utf-8', errors='strict')
        f = open(filepath, "w", encoding="utf-8")
        json.dump(tags, f, indent=4, sort_keys=True)
        f.close()

    @staticmethod
    def _write_wiki_json_file(root_dir, wiki):
        filepath = os.path.join(root_dir, wiki['entity_id'] + ".json").encode(encoding='utf-8', errors='strict')
        f = open(filepath, "w", encoding="utf-8")
        json.dump(wiki, f, indent=4, sort_keys=True)
        f.close()

    @staticmethod
    def _replace_special_characters(fs_item):
        return fs_item.replace("\\", "_").replace("/", "_")

    @staticmethod
    def _get_fs_path(object_path):
        fs_path = ""
        for item in object_path:
            fs_path = os.path.join(fs_path, EnvFileWriter._replace_special_characters(item))
        return fs_path
