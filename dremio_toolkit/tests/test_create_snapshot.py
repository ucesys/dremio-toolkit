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

import tempfile
import os
import json
from datetime import datetime

from dremio_toolkit.logger import Logger
from dremio_toolkit.create_snapshot import create_snapshot
from dremio_toolkit.testing.mock_env_api import MockEnvApi
from dremio_toolkit.testing.utils import load_snapshot


def test_create_snapshot():
    logger = Logger(level="DEBUG", verbose=True)
    env_api = MockEnvApi()
    expected_snapshot = load_snapshot()
    test_dt = datetime.fromisoformat("2023-01-01")

    with tempfile.NamedTemporaryFile(mode='w') as tmp_file:  # open file
        create_snapshot(
            env_api=env_api, logger=logger, output_mode='FILE', output_path=tmp_file.name,
            report_filename=None, report_delimiter='\n'
        )

        assert os.path.isfile(tmp_file.name), "Snapshot does not exist"

        created_snapshot = json.load(open(tmp_file.name))
        created_snapshot["data"]["dremio_environment"]["timestamp_utc"] = expected_snapshot["data"]["dremio_environment"]["timestamp_utc"]
        assert created_snapshot == expected_snapshot, "Snapshot is different than expected"
