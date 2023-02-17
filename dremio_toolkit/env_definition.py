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

from dataclasses import dataclass
from typing import Dict, Any


@dataclass(frozen=True)
class EnvDefinition:
    containers: list[Dict[str, Any]]
    homes: list[Dict[str, Any]]
    sources: list[Dict[str, Any]]
    spaces: list[Dict[str, Any]]
    folders: list[Dict[str, Any]]
    vds_list: list[Dict[str, Any]]
    reflections: list[Dict[str, Any]]
    rules: list[Dict[str, Any]]
    queues: list[Dict[str, Any]]
    votes: list[Dict[str, Any]]
    files: list[Dict[str, Any]]
    tags: list[Dict[str, Any]]
    wikis: list[Dict[str, Any]]
    referenced_users: list[Dict[str, Any]]
    referenced_groups: list[Dict[str, Any]]
    referenced_roles: list[Dict[str, Any]]
    endpoint: str
