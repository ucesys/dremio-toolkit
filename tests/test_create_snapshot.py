import json

from env_file_writer import EnvFileWriter
from env_api import EnvApi
from env_reader import EnvReader
from logger import Logger
from mock_env_api import MockEnvApi

def test_create_snapshot():
    logger = Logger(level="WARN", verbose="")
    env_api = MockEnvApi()
    env_reader = EnvReader(env_api, logger)
    env_def = env_reader.read_dremio_environment()
    env_writer = EnvFileWriter()
    env_writer.save_dremio_environment(env_api, env_def, "test2.json")