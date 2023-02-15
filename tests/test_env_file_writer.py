import json

from env_file_writer import EnvFileWriter
from env_api import EnvApi
from env_definition import EnvDefinition
from mock_env_definition import MockEnvDefinition

class MockEnvApi(EnvApi):
    def __init__(self):
        pass

    def get_env_endpoint(self) -> str:
        return "http://localhost:9047/"

def test_env_file_writer():
    env_api = MockEnvApi()
    env_def = MockEnvDefinition()
    env_writer = EnvFileWriter()
    env_writer.save_dremio_environment(env_api, env_def, "test.json")