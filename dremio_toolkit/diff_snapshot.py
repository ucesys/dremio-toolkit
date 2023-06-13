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
from dremio_toolkit.context import Context


def parse_args():
    # Process arguments
    arg_parser = argparse.ArgumentParser(
        description='diff_snapshot is a part of the Dremio Toolkit. It reads two Dremio enviroment snapshots '
                    'from files and produces report on differences.',
        epilog='Copyright UCE Systems Corp. For any assistance contact developer at dremio@ucesys.com'
    )
    arg_parser.add_argument("-m", "--file-mode", help="Whether base and comp environments are in a single JSON "
                                                      "file or a directory with individual files for each object.",
                            required=False, choices=['FILE', 'DIR'], default='FILE')
    arg_parser.add_argument("-b", "--base-path", help="Json file name or a directory name with snapshot of a 'base' Dremio environment.", required=True)
    arg_parser.add_argument("-c", "--comp-path", help="Json file name or a directory name with snapshot of a 'comp' Dremio environment.", required=True)
    arg_parser.add_argument("-r", "--report-filename", help="Json file name for the 'diff' report.", required=True)
    arg_parser.add_argument("-l", "--log-level", help="Set Log Level to DEBUG, INFO, WARN, ERROR.",
                            choices=['ERROR', 'WARN', 'INFO', 'DEBUG'], default='WARN')
    arg_parser.add_argument("-v", "--verbose", help="Set Log to verbose to print object definitions instead of object IDs.",
                            required=False, default=False, action='store_true')
    arg_parser.add_argument("-f", "--log-filename", help="Set Log to write to a specified file instead of STDOUT.",
                            required=False)
    parsed_args = arg_parser.parse_args()
    return parsed_args


def diff_snapshot(ctx: Context):
    # Process command
    file_reader = EnvFileReader()
    base_env_def = file_reader.read_dremio_source_environment(context)
    comp_env_def = file_reader.read_dremio_target_environment(context)
    env_diff = EnvDiff(ctx)
    env_diff.diff_snapshot(base_env_def, comp_env_def)
    env_diff.write_diff_report()

    # Return process status to the OS
    context.get_logger().finish_process_status_reporting()
    if context.get_logger().get_error_count() > 0:
        exit(Context.NON_FATAL_EXIT_CODE)

if __name__ == '__main__':
    args = parse_args()

    context = Context(Context.CMD_DIFF_SNAPSHOT)
    context.init_logger(log_level=args.log_level, log_verbose=args.verbose, log_filepath=args.log_filename)
    context.set_source(input_mode=args.file_mode, input_path=args.base_path)
    context.set_target(output_mode=args.file_mode, output_path=args.comp_path)
    context.set_report(report_filepath=args.report_filename)
    diff_snapshot(context)
