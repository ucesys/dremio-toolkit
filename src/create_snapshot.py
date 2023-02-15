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

from DremioToolkitEnvApi import DremioToolkitEnvApi
from DremioToolkitEnvReader import DremioToolkitEnvReader
from DremioToolkitLogger import DremioToolkitLogger
from DremioToolkitEnvFileWriter import DremioToolkitEnvFileWriter
import argparse

if __name__ == '__main__':

    # Process arguments
    arg_parser = argparse.ArgumentParser(
        description='create_snapshot is a part of the Dremio Toolkit. It reads a Dremio enviroment via API and saves it as a JSON file.',
        epilog='Copyright UCE Systems Corp. For any assistance contact developer at dremio@ucesys.com'
    )
    arg_parser.add_argument("-d", "--dremio-environment-url", help="URL to Dremio environment.", required=True)
    arg_parser.add_argument("-u", "--user", help="User name. User must be a Dremio admin.", required=True)
    arg_parser.add_argument("-p", "--password", help="User password.", required=True)
    arg_parser.add_argument("-o", "--output-filename", help="Json file name to save Dremio environment.", required=True)
    arg_parser.add_argument("-l", "--log-level", help="Set Log Level to DEBUG, INFO, WARN, ERROR.",
                            choices=['ERROR', 'WARN', 'INFO', 'DEBUG'], default='WARN')
    arg_parser.add_argument("-v", "--verbose", help="Set Log to verbose to print object definitions instead of object IDs.",
                            required=False, default=False, action='store_true')
    args = arg_parser.parse_args()

    # Process command
    dremio_tk_logger = DremioToolkitLogger(level=args.log_level, verbose=args.verbose)
    dremio_env = DremioToolkitEnvApi(args.dremio_environment_url, args.user, args.password, dremio_tk_logger)
    dremio_env_reader = DremioToolkitEnvReader(dremio_env, dremio_tk_logger)
    dremio_env_def = dremio_env_reader.read_dremio_environment()
    dremio_env_writer = DremioToolkitEnvFileWriter()
    dremio_env_writer.save_dremio_environment(dremio_env, dremio_env_def, args.output_filename)

    # Return process status to the OS
    if dremio_tk_logger.get_error_count() == 0:
        exit(0)
    else:
        exit(1)

