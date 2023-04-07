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


# Data Class to contain definition of Dremio Environment
class EnvDefinition:
    containers = []
    sources = []
    spaces = []
    folders = []
    vds_list = []
    vds_parents = []
    reflections = []
    rules = []
    queues = []
    votes = []
    files = []
    tags = []
    wikis = []
    referenced_users = []
    referenced_groups = []
    referenced_roles = []
    # dremio_environment
    file_version = 1
    endpoint = None
    timestamp_utc = None

