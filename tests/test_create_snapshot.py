import tempfile
import os
import json
from datetime import datetime

from dremio_toolkit.logger import Logger
from dremio_toolkit.create_snapshot import create_snapshot

from mock_env_api import MockEnvApi


def test_create_snapshot():
    logger = Logger(level="ERROR", verbose=False)
    env_api = MockEnvApi()
    expected_path = "tests/expected_snapshot.json"
    test_dt = datetime.fromisoformat("2023-01-01")

    with tempfile.NamedTemporaryFile(mode='w') as tmp_file:  # open file
        create_snapshot(env_api=env_api, logger=logger, output_file=tmp_file.name, datetime_utc=test_dt)

        assert os.path.isfile(tmp_file.name), "Snapshot does not exist"
        assert json.load(open(tmp_file.name)) == json.load(open(expected_path)), "Snapshot is different than expected"
