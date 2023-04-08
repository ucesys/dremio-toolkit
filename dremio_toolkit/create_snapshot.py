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
from dremio_toolkit.env_reader import EnvReader
from dremio_toolkit.logger import Logger
from dremio_toolkit.env_file_writer import EnvFileWriter


def parse_args():
    arg_parser = argparse.ArgumentParser(
        description='create_snapshot is a part of the Dremio Toolkit. It reads a Dremio enviroment via API and '
                    'saves the snapshot as a JSON file.',
        epilog='Copyright UCE Systems Corp. For any assistance contact developer at dremio@ucesys.com'
    )
    arg_parser.add_argument("-d", "--dremio-environment-url", help="URL to Dremio environment.", required=True)
    arg_parser.add_argument("-u", "--user", help="User name. User must be a Dremio admin.", required=True)
    arg_parser.add_argument("-p", "--password", help="User password.", required=True)
    arg_parser.add_argument("-m", "--output-mode", help="Whether create a single output JSON file or a directory with "
                                                        "individual files for each object.", required=False,
                                                        choices=['FILE', 'DIR'], default='FILE')
    arg_parser.add_argument("-o", "--output-path", help="Json file name or a directory name to save Dremio environment.", required=True)
    arg_parser.add_argument("-r", "--report-filename", help="CSV file name for the exception report.", required=False)
    arg_parser.add_argument("-e", "--report-delimiter", help="Delimiter to use in the exception report. Default is tab.", required=False, default='\t')
    arg_parser.add_argument("-l", "--log-level", help="Set Log Level to DEBUG, INFO, WARN, ERROR.",
                            choices=['ERROR', 'WARN', 'INFO', 'DEBUG'], default='WARN')
    arg_parser.add_argument("-v", "--verbose", help="Set Log to verbose to print object definitions instead of object IDs.",
                            required=False, default=False, action='store_true')
    arg_parser.add_argument("-f", "--log-filename", help="Set Log to write to a specified file instead of STDOUT.",
                            required=False)
    parsed_args = arg_parser.parse_args()
    if parsed_args.report_filename is None:
        print("report-filename argument has not been specified. Exception report will not be produced.")
    return parsed_args


def create_snapshot(env_api, logger, output_mode, output_path, report_filename, report_delimiter):
    env_reader = EnvReader(env_api, logger)
    env_def = env_reader.read_dremio_environment()

    if report_filename is not None:
        env_reader.write_exception_report(report_filename, report_delimiter)

    EnvFileWriter.save_dremio_environment(env_def, output_mode, output_path, logger)

    logger.finish_process_status_reporting()
    if logger.get_error_count() > 0:
        exit(Utils.NON_FATAL_EXIT_CODE)


if __name__ == '__main__':
    args = parse_args()

    logger = Logger(level=args.log_level, verbose=args.verbose, log_file=args.log_filename)
    env_api = EnvApi(args.dremio_environment_url, args.user, args.password, logger)

    create_snapshot(
        env_api=env_api, logger=logger, output_mode=args.output_mode, output_path=args.output_path,
        report_filename=args.report_filename, report_delimiter=args.report_delimiter
    )
