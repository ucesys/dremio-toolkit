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


class DremioToolkitUtils:

    def __init__(self):
        return

    # Pop all items from container definition
    def pop_it(self, container, items):
        for item in items:
            if item in container:
                container.pop(item)

    # Appends a list if the item does not exist in the list yet
    def append_unique_list(self, list_object, item_object):
        if item_object not in list_object:
            list_object.append(item_object)