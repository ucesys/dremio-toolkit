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
import tempfile

from dremio_toolkit.env_file_writer import EnvFileWriter
from dremio_toolkit.testing.mock_env_definition import mock_env_definition
from dremio_toolkit.testing.utils import load_snapshot


def test_env_file_writer():
    env_def = mock_env_definition()
    expected_snapshot = load_snapshot()

    with tempfile.NamedTemporaryFile(mode='w') as tmp_file:
        EnvFileWriter.save_dremio_environment(env_def, tmp_file.name)

        assert os.path.isfile(tmp_file.name), "Snapshot does not exist"
        assert json.load(open(tmp_file.name)) == expected_snapshot, "Snapshot is different than expected"