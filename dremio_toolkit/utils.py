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


class Utils:

    def __init__(self):
        return

    # Pop all items from container definition
    @staticmethod
    def pop_it(container, items):
        for item in items:
            if item in container:
                container.pop(item)

    # Appends a list if the item does not exist in the list yet
    @staticmethod
    def append_unique_list(list_object, item_object):
        if item_object not in list_object:
            list_object.append(item_object)

    # Convert List path to a String if required
    @staticmethod
    def get_str_path(path):
        # Only normalize lists, do not modify strings
        if type(path) != list:
            return path
        str_path = ""
        for item in path:
            str_path = str_path + item + "/"
        return str_path[:-1]

    @staticmethod
    def get_fully_qualified_path(path, sql_context):
        path = Utils.get_str_path(path)
        if '/' not in path and sql_context is not None and sql_context != "":
            path = Utils.get_str_path(sql_context) + "/" + path
        return path

    @staticmethod
    def get_sql_context(entity):
        return entity["sqlContext"] if "sqlContext" in entity else None

    @staticmethod
    def is_vds(entity):
        return entity['entityType'] == 'dataset' and entity['type'] == 'VIRTUAL_DATASET'

    @staticmethod
    def is_pds(entity):
        return entity['entityType'] == 'dataset' and entity['type'] == 'PHYSICAL_DATASET'

    @staticmethod
    def get_absolute_path(path, sql_context):
        path = Utils.get_str_path(path)
        if '/' not in path and sql_context is not None and sql_context != "":
            path = Utils.get_str_path(sql_context) + "/" + path
        return path
