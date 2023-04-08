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
    def __init__(self):
        self.containers = []
        self.sources = []
        self.spaces = []
        self.folders = []
        self.vds_list = []
        self.vds_parents = []
        self.reflections = []
        self.rules = []
        self.queues = []
        self.votes = []
        self.files = []
        self.tags = []
        self.wikis = []
        self.referenced_users = []
        self.referenced_groups = []
        self.referenced_roles = []
        # dremio_environment
        self.file_version = None
        self.endpoint = None
        self.timestamp_utc = None

