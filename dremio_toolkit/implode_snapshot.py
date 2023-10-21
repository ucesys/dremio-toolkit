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
from dremio_toolkit.env_file_writer import EnvFileWriter
from dremio_toolkit.logger import Logger
from dremio_toolkit.env_file_reader import EnvFileReader
from dremio_toolkit.context import Context


def parse_args():
    # Process arguments
    arg_parser = argparse.ArgumentParser(
        description='implode_snapshot is a part of the Dremio Toolkit. It reads a Dremio snapshot from a directory with '
                    'JSON files and saves it into a single JSON file.',
        epilog='Copyright UCE Systems Corp. For any assistance contact developer at dremio@ucesys.com'
    )
    arg_parser.add_argument("-i", "--input-path", help="Directory name with snapshot of a Dremio environment.", required=True)
    arg_parser.add_argument("-o", "--output-path", help="Target filename for saving the snapshot as a JSON file.", required=True)
    arg_parser.add_argument("-l", "--log-level", help="Set Log Level to DEBUG, INFO, WARN, ERROR.",
                            choices=['ERROR', 'WARN', 'INFO', 'DEBUG'], default='WARN')
    arg_parser.add_argument("-v", "--verbose", help="Set Log to verbose to print object definitions instead of object IDs.",
                            required=False, default=False, action='store_true')
    arg_parser.add_argument("-f", "--log-filename", help="Set Log to write to a specified file instead of STDOUT.",
                            required=False)
    parsed_args = arg_parser.parse_args()
    return parsed_args


def implode_snapshot(ctx: Context):
    logger = ctx.get_logger()
    # Process command
    file_reader = EnvFileReader()
    env_def = file_reader.read_dremio_source_environment(ctx)
    EnvFileWriter.save_dremio_environment(ctx, env_def)

    # Return process status to the OS
    logger.finish_process_status_reporting()
    if logger.get_error_count() > 0:
        exit(Context.NON_FATAL_EXIT_CODE)


if __name__ == '__main__':
    print("dremio-toolkit version " + str(Context.APP_VERSION))

    args = parse_args()

    context = Context(Context.CMD_IMPLODE_SNAPSHOT)
    context.init_logger(log_level=args.log_level, log_verbose=args.verbose, log_filepath=args.log_filename)
    context.set_source(input_mode=Context.PATH_MODE_DIR, input_path=args.input_path)
    context.set_target(output_mode=Context.PATH_MODE_FILE, output_path=args.output_path)

    implode_snapshot(context)
