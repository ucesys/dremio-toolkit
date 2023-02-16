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

import argparse
from datetime import datetime

from .env_api import EnvApi
from .env_reader import EnvReader
from .logger import Logger
from .env_file_writer import EnvFileWriter


def create_snapshot(env_api: EnvApi, logger: Logger, output_file: str, datetime_utc: datetime) -> None:
    env_reader = EnvReader(env_api, logger)
    env_def = env_reader.read_dremio_environment()

    env_writer = EnvFileWriter()
    env_writer.save_dremio_environment(env_api, env_def, output_file, datetime_utc=datetime_utc)

    assert logger.get_error_count() == 0, "Errors encountered during snapshot creation"


if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser(
        description='create_snapshot is a part of the Dremio Toolkit. It reads a Dremio enviroment via API and saves it as a JSON file.',
        epilog='Copyright UCE Systems Corp. For any assistance contact developer at dremio@ucesys.com'
    )
    arg_parser.add_argument("-d", "--dremio-environment-url", help="URL to Dremio environment.", required=True)
    arg_parser.add_argument("-u", "--user", help="User name. User must be a Dremio admin.", required=True)
    arg_parser.add_argument("-p", "--password", help="User password.", required=True)
    arg_parser.add_argument("-o", "--output-file", help="Json file name to save Dremio environment.", required=True)
    arg_parser.add_argument("-l", "--log-level", help="Set Log Level to DEBUG, INFO, WARN, ERROR.",
                            choices=['ERROR', 'WARN', 'INFO', 'DEBUG'], default='WARN')
    arg_parser.add_argument("-v", "--verbose", help="Set Log to verbose to print object definitions instead of object IDs.",
                            required=False, default=False, action='store_true')
    args = arg_parser.parse_args()

    logger = Logger(level=args.log_level, verbose=args.verbose)
    env_api = EnvApi(args.dremio_environment_url, args.user, args.password, logger)

    create_snapshot(env_api=env_api, logger=logger, output_file=args.output_file, datetime_utc=datetime.utcnow())



