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


class EnvDefinition:
    containers: list
    homes: list
    sources: list
    spaces: list
    folders: list
    vds_list: list
    reflections: list
    rules: list
    queues: list
    votes: list
    files: list
    tags: list
    wikis: list
    referenced_users: list
    referenced_groups: list
    referenced_roles: list
    endpoint: str

    def __init__(self, containers,
                 homes, spaces, sources, folders, vds_list, reflections, rules, queues, votes, endpoint, tags,
                 wikis, referenced_users, referenced_groups, referenced_roles, files):
        self.containers = containers
        self.homes = homes
        self.sources = sources
        self.spaces = spaces
        self.folders = folders
        self.vds_list = vds_list
        self.reflections = reflections
        self.rules = rules
        self.queues = queues
        self.votes = votes
        self.files = files
        self.tags = tags
        self.wikis = wikis
        self.referenced_users = referenced_users
        self.referenced_groups = referenced_groups
        self.referenced_roles = referenced_roles
        self.endpoint = endpoint
