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
from dremio_toolkit.utils import Utils
from dremio_toolkit.env_api import EnvApi
from dremio_toolkit.env_writer import EnvWriter
from dremio_toolkit.logger import Logger
from dremio_toolkit.env_file_reader import EnvFileReader


def parse_args():
    # Process arguments
    arg_parser = argparse.ArgumentParser(
        description='push_snapshot is a part of the Dremio Toolkit. It reads a Dremio snapshot from a file or '
                    'directory and pushes it into a Dremio environment via API.',
        epilog='Copyright UCE Systems Corp. For any assistance contact developer at dremio@ucesys.com'
    )
    arg_parser.add_argument("-d", "--dremio-environment-url", help="URL to Dremio environment.", required=True)
    arg_parser.add_argument("-u", "--user", help="User name. User must be a Dremio admin.", required=True)
    arg_parser.add_argument("-p", "--password", help="User password.", required=True)
    arg_parser.add_argument("-m", "--input-mode", help="Whether read a single inout JSON file or a directory with "
                            "individual files for each object.", required=False, choices=['FILE', 'DIR'], default='FILE')
    arg_parser.add_argument("-i", "--input-path", help="Json file name or a directory name with a snapshot of a Dremio environment.", required=True)
    arg_parser.add_argument("-y", "--dry-run", help="Whether it's a dry run or changes should be made to the target "
                                                    "Dremio environment.", required=False, default=False, action='store_true')
    arg_parser.add_argument("-r", "--report-filename", help="CSV file name for the exception' report.", required=False)
    arg_parser.add_argument("-e", "--report-delimiter", help="Delimiter to use in the exception report. Default is tab.",
                            required=False, default='\t')
    arg_parser.add_argument("-l", "--log-level", help="Set Log Level to DEBUG, INFO, WARN, ERROR.",
                            choices=['ERROR', 'WARN', 'INFO', 'DEBUG'], default='WARN')
    arg_parser.add_argument("-v", "--verbose", help="Set Log to verbose to print object definitions instead of object IDs.",
                            required=False, default=False, action='store_true')
    arg_parser.add_argument("-f", "--log-filename", help="Set Log to write to a specified file instead of STDOUT.",
                            required=False)
    parsed_args = arg_parser.parse_args()
    if parsed_args.report_filename is None:
        print("Exception report file name has not been supplied with report-filename argument. Report file will not be produced.")
    return parsed_args


def push_snapshot(input_mode, input_path, dremio_environment_url, user, password, dry_run,
                  report_filename, report_delimiter, log_level, log_filename, verbose):
    # Process command
    logger = Logger(level=log_level, verbose=verbose, log_file=log_filename)
    file_reader = EnvFileReader()
    env_def = file_reader.read_dremio_environment(input_mode, input_path)
    env_api = EnvApi(dremio_environment_url, user, password, logger, dry_run=dry_run)
    env_writer = EnvWriter(env_api, env_def, logger)
    env_writer.write_dremio_environment()
    env_writer.write_exception_report(report_filename, report_delimiter)

    # Return process status to the OS
    logger.finish_process_status_reporting()
    if logger.get_error_count() > 0:
        exit(Utils.NON_FATAL_EXIT_CODE)


if __name__ == '__main__':
    args = parse_args()
    push_snapshot(args.input_mode, args.input_path, args.dremio_environment_url, args.user, args.password, bool(args.dry_run),
                    args.report_filename, args.report_delimiter, args.log_level, args.log_filename, args.verbose)

