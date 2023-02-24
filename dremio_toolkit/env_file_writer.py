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
from datetime import datetime

from dremio_toolkit.env_definition import EnvDefinition


class EnvFileWriter:
    @staticmethod
    def save_dremio_environment(env_def: EnvDefinition, output_file: str, datetime_utc: datetime) -> None:
        if os.path.isfile(output_file):
            os.remove(output_file)

        env_snapshot = {
            "data": [
                {'dremio_environment': [
                    {'file_version': '1.0'},
                    {"endpoint": env_def.endpoint},
                    {'timestamp_utc': str(datetime_utc)}
                ]},
                {'containers': env_def.containers},
                {'homes': env_def.homes},
                {'sources': env_def.sources},
                {'spaces': env_def.spaces},
                {'folders': env_def.folders},
                {'vds': env_def.vds_list},
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
