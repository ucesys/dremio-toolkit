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

from utils import Utils


class EnvFileWriter:
    _utils = None

    def __init__(self):
        self._utils = Utils()
        return

    def save_dremio_environment(self, env_api_wrapper, dremio_env_def, output_filename):
        if os.path.isfile(output_filename):
            os.remove(output_filename)
        f = open(output_filename, "w", encoding="utf-8")
        f.write('{ "data": [')
        json.dump({'dremio_environment': [{'file_version': '1.0'}, {"endpoint": env_api_wrapper.get_env_endpoint()},
                                          {'timestamp_utc': str(datetime.utcnow())}]}, f, indent=4, sort_keys=True)
        f.write(',\n')
        json.dump({'containers': dremio_env_def.containers}, f, indent=4, sort_keys=True)
        f.write(',\n')
        json.dump({'homes': dremio_env_def.homes}, f, indent=4, sort_keys=True)
        f.write(',\n')
        json.dump({'sources': dremio_env_def.sources}, f, indent=4, sort_keys=True)
        f.write(',\n')
        json.dump({'spaces': dremio_env_def.spaces}, f, indent=4, sort_keys=True)
        f.write(',\n')
        json.dump({'folders': dremio_env_def.folders}, f, indent=4, sort_keys=True)
        f.write(',\n')
        json.dump({'vds': dremio_env_def.vds_list}, f, indent=4, sort_keys=True)
        f.write(',\n')
        json.dump({'files': dremio_env_def.files}, f, indent=4, sort_keys=True)
        f.write(',\n')
        json.dump({'reflections': dremio_env_def.reflections}, f, indent=4, sort_keys=True)
        f.write(',\n')
        json.dump({'queues': dremio_env_def.queues}, f, indent=4, sort_keys=True)
        f.write(',\n')
        json.dump({'rules': dremio_env_def.rules}, f, indent=4, sort_keys=True)
        f.write(',\n')
        json.dump({'tags': dremio_env_def.tags}, f, indent=4, sort_keys=True)
        f.write(',\n')
        json.dump({'wikis': dremio_env_def.wikis}, f, indent=4, sort_keys=True)
        f.write(',\n')
        json.dump({'votes': dremio_env_def.votes}, f, indent=4, sort_keys=True)
        f.write(',\n')
        json.dump({'referenced_users': dremio_env_def.referenced_users}, f, indent=4, sort_keys=True)
        f.write(',\n')
        json.dump({'referenced_groups': dremio_env_def.referenced_groups}, f, indent=4, sort_keys=True)
        f.write(',\n')
        json.dump({'referenced_roles': dremio_env_def.referenced_roles}, f, indent=4, sort_keys=True)
        f.write(' ] }')
        f.close()
