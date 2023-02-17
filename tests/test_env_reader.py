from dremio_toolkit.logger import Logger
from dremio_toolkit.env_reader import EnvReader
from mock_env_api import MockEnvApi
from mock_env_definition import mock_env_defnition


def test_read_dremio_environment():
    logger = Logger(level="ERROR", verbose=False)
    env_api = MockEnvApi()
    env_reader = EnvReader(env_api, logger)
    expected_env_def = mock_env_defnition()

    env_def = env_reader.read_dremio_environment()

    assert env_def.containers == expected_env_def.containers
    assert env_def.homes == expected_env_def.homes
    assert env_def.sources == expected_env_def.sources
    assert env_def.spaces == expected_env_def.spaces
    assert env_def.folders == expected_env_def.folders
    assert env_def.vds_list == expected_env_def.vds_list
    assert env_def.reflections == expected_env_def.reflections
    assert env_def.queues == expected_env_def.queues
    assert env_def.rules == expected_env_def.rules
    assert env_def.tags == expected_env_def.tags
    assert env_def.wikis == expected_env_def.wikis
    assert env_def.referenced_users == expected_env_def.referenced_users
    assert env_def.referenced_roles == expected_env_def.referenced_roles
