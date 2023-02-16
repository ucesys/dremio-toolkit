import json
import os
from datetime import datetime
import tempfile

from dremio_toolkit.env_file_writer import EnvFileWriter
from mock_env_definition import MockEnvDefinition


def test_env_file_writer():
    env_def = MockEnvDefinition()
    env_writer = EnvFileWriter()

    expected_path = "tests/expected_snapshot.json"
    test_dt = datetime.fromisoformat("2023-01-01")

    with tempfile.NamedTemporaryFile(mode='w') as tmp_file:
        env_writer.save_dremio_environment(env_def, tmp_file.name, test_dt)

        assert os.path.isfile(tmp_file.name), "Snapshot does not exist"
        assert json.load(open(tmp_file.name)) == json.load(open(expected_path)), "Snapshot is different than expected"