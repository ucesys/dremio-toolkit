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

from dremio_toolkit.logger import Logger
from dremio_toolkit.env_reader import EnvReader
from dremio_toolkit.testing.mock_env_api import MockEnvApi
from dremio_toolkit.testing.mock_env_definition import mock_env_definition
from dremio_toolkit.context import Context


def test_read_dremio_environment():
    context = Context()
    context.init_logger(log_level="WARN", log_verbose=False)
    context.set_source_env_api(MockEnvApi())
    env_reader = EnvReader(context)
    expected_env_def = mock_env_definition()
    env_def = env_reader.read_dremio_environment()

    assert env_def.containers == expected_env_def.containers
    assert env_def.sources == expected_env_def.sources
    assert env_def.spaces == expected_env_def.spaces
    assert env_def.folders == expected_env_def.folders
    assert env_def.vds_list == expected_env_def.vds_list
    assert env_def.vds_parents == expected_env_def.vds_parents
    assert env_def.reflections == expected_env_def.reflections
    assert env_def.queues == expected_env_def.queues
    assert env_def.rules == expected_env_def.rules
    assert env_def.tags == expected_env_def.tags
    assert env_def.wikis == expected_env_def.wikis
    assert env_def.referenced_users == expected_env_def.referenced_users
    assert env_def.referenced_roles == expected_env_def.referenced_roles
