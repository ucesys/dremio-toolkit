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
from dremio_toolkit.env_diff import EnvDiff
from dremio_toolkit.logger import Logger
from dremio_toolkit.env_file_reader import EnvFileReader

def parse_args():
    # Process arguments
    arg_parser = argparse.ArgumentParser(
        description='create_snapshot is a part of the Dremio Toolkit. It reads a Dremio enviroment via API and saves it as a JSON file.',
        epilog='Copyright UCE Systems Corp. For any assistance contact developer at dremio@ucesys.com'
    )
    arg_parser.add_argument("-b", "--base-filename", help="Json file name with snapshot of the 'base' Dremio environment.", required=True)
    arg_parser.add_argument("-c", "--comp-filename", help="Json file name with snapshot of the 'comp' Dremio environment.", required=True)
    arg_parser.add_argument("-r", "--report-filename", help="Json file name for the 'diff' report.", required=True)
    arg_parser.add_argument("-l", "--log-level", help="Set Log Level to DEBUG, INFO, WARN, ERROR.",
                            choices=['ERROR', 'WARN', 'INFO', 'DEBUG'], default='WARN')
    arg_parser.add_argument("-v", "--verbose", help="Set Log to verbose to print object definitions instead of object IDs.",
                            required=False, default=False, action='store_true')
    arg_parser.add_argument("-f", "--log-filename", help="Set Log to write to a specified file instead of STDOUT.",
                            required=False)
    parsed_args = arg_parser.parse_args()
    return parsed_args

def diff_snapshot(base_filename, comp_filename, report_filename, log_level, log_filename, verbose):
    # Process command
    logger = Logger(level=log_level, verbose=verbose, log_file=log_filename)
    file_reader = EnvFileReader()
    base_env_def = file_reader.read_dremio_environment(base_filename)
    comp_env_def = file_reader.read_dremio_environment(comp_filename)
    env_diff = EnvDiff(logger)
    env_diff.diff_snapshot(base_env_def, comp_env_def)
    env_diff.write_diff_report(report_filename)

    # Return process status to the OS
    logger.finish_process_status_reporting()
    if logger.get_error_count() > 0:
        exit(Utils.NON_FATAL_EXIT_CODE)


if __name__ == '__main__':
    args = parse_args()
    diff_snapshot(args.base_filename, args.comp_filename, args.report_filename,
                  args.log_level, args.log_filename, args.verbose)
