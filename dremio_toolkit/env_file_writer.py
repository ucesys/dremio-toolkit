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

from .utils import Utils
from .env_definition import EnvDefinition


class EnvFileWriter:
    _utils = None

    def __init__(self):
        self._utils = Utils()
        return

    def save_dremio_environment(self, env_def: EnvDefinition, output_file: str, datetime_utc: datetime):
        if os.path.isfile(output_file):
            os.remove(output_file)
        f = open(output_file, "w", encoding="utf-8")
        f.write('{ "data": [')
        json.dump({'dremio_environment': [{'file_version': '1.0'}, {"endpoint": env_def.endpoint},
                                          {'timestamp_utc': str(datetime_utc)}]}, f, indent=4, sort_keys=True)
        f.write(',\n')
        json.dump({'containers': env_def.containers}, f, indent=4, sort_keys=True)
        f.write(',\n')
        json.dump({'homes': env_def.homes}, f, indent=4, sort_keys=True)
        f.write(',\n')
        json.dump({'sources': env_def.sources}, f, indent=4, sort_keys=True)
        f.write(',\n')
        json.dump({'spaces': env_def.spaces}, f, indent=4, sort_keys=True)
        f.write(',\n')
        json.dump({'folders': env_def.folders}, f, indent=4, sort_keys=True)
        f.write(',\n')
        json.dump({'vds': env_def.vds_list}, f, indent=4, sort_keys=True)
        f.write(',\n')
        json.dump({'files': env_def.files}, f, indent=4, sort_keys=True)
        f.write(',\n')
        json.dump({'reflections': env_def.reflections}, f, indent=4, sort_keys=True)
        f.write(',\n')
        json.dump({'queues': env_def.queues}, f, indent=4, sort_keys=True)
        f.write(',\n')
        json.dump({'rules': env_def.rules}, f, indent=4, sort_keys=True)
        f.write(',\n')
        json.dump({'tags': env_def.tags}, f, indent=4, sort_keys=True)
        f.write(',\n')
        json.dump({'wikis': env_def.wikis}, f, indent=4, sort_keys=True)
        f.write(',\n')
        json.dump({'votes': env_def.votes}, f, indent=4, sort_keys=True)
        f.write(',\n')
        json.dump({'referenced_users': env_def.referenced_users}, f, indent=4, sort_keys=True)
        f.write(',\n')
        json.dump({'referenced_groups': env_def.referenced_groups}, f, indent=4, sort_keys=True)
        f.write(',\n')
        json.dump({'referenced_roles': env_def.referenced_roles}, f, indent=4, sort_keys=True)
        f.write(' ] }')
        f.close()
