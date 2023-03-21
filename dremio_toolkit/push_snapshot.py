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

from dremio_toolkit.env_api import EnvApi
from dremio_toolkit.env_writer import EnvWriter
from dremio_toolkit.logger import Logger
from dremio_toolkit.env_file_reader import EnvFileReader

if __name__ == '__main__':

    # Process arguments
    arg_parser = argparse.ArgumentParser(
        description='create_snapshot is a part of the Dremio Toolkit. It reads a Dremio enviroment via API and saves it as a JSON file.',
        epilog='Copyright UCE Systems Corp. For any assistance contact developer at dremio@ucesys.com'
    )
    arg_parser.add_argument("-d", "--dremio-environment-url", help="URL to Dremio environment.", required=True)
    arg_parser.add_argument("-u", "--user", help="User name. User must be a Dremio admin.", required=True)
    arg_parser.add_argument("-p", "--password", help="User password.", required=True)
    arg_parser.add_argument("-i", "--input-filename", help="Json file name with snapshot of Dremio environment.", required=True)
    arg_parser.add_argument("-y", "--dry-run", help="Whether it's a dry run or changes should be made to the target "
                                                    "Dremio environment.", required=False, default=False)
    arg_parser.add_argument("-r", "--report-filename", help="CSV file name for the exception' report.", required=False)
    arg_parser.add_argument("-e", "--report-delimiter", help="Delimiter to use in the exception report. Default is tab.", required=False, default='\t')
    arg_parser.add_argument("-l", "--log-level", help="Set Log Level to DEBUG, INFO, WARN, ERROR.",
                            choices=['ERROR', 'WARN', 'INFO', 'DEBUG'], default='WARN')
    arg_parser.add_argument("-v", "--verbose", help="Set Log to verbose to print object definitions instead of object IDs.",
                            required=False, default=False, action='store_true')
    arg_parser.add_argument("-f", "--log-file", help="Set Log to write to a specified file instead of STDOUT.",
                            required=False)
    args = arg_parser.parse_args()

    # Process command
    logger = Logger(level=args.log_level, verbose=args.verbose, log_file=args.log_file)
    file_reader = EnvFileReader()
    env_def = file_reader.read_dremio_environment(args.input_filename)
    env_api = EnvApi(args.dremio_environment_url, args.user, args.password, logger, dry_run=args.dry_run)
    env_writer = EnvWriter(env_api, env_def, logger)
    env_writer.write_dremio_environment()
    env_writer.write_exception_report(args.report_filename)

    # Return process status to the OS
    logger.finish_process_status_reporting()
    if logger.get_error_count() > 0:
        exit(1)

