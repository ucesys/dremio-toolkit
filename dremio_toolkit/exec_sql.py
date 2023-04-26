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
from dremio_toolkit.logger import Logger
import os
import json


def parse_args():
    # Process arguments
    arg_parser = argparse.ArgumentParser(
        description='exec_sql is a part of the Dremio Toolkit. Executes SQL code SQL code from a specified file. The file can contain a number of SQL commands which can be separated with ";".',
        epilog='Copyright UCE Systems Corp. For any assistance contact developer at dremio@ucesys.com'
    )
    arg_parser.add_argument("-d", "--dremio-environment-url", help="URL to Dremio environment.", required=True)
    arg_parser.add_argument("-u", "--user", help="User name. User must be a Dremio admin.", required=True)
    arg_parser.add_argument("-p", "--password", help="User password.", required=True)
    arg_parser.add_argument("-s", "--sql-filename", help="File name with SQL code.", required=True)
    arg_parser.add_argument("-r", "--report-filename", help="CSV file name for the JSON exception' report.", required=False)
    arg_parser.add_argument("-l", "--log-level", help="Set Log Level to DEBUG, INFO, WARN, ERROR.",
                            choices=['ERROR', 'WARN', 'INFO', 'DEBUG'], default='WARN')
    arg_parser.add_argument("-f", "--log-filename", help="Set Log to write to a specified file instead of STDOUT.",
                            required=False)
    parsed_args = arg_parser.parse_args()
    if parsed_args.report_filename is None:
        print("report-filename argument has not been specified. Exception report will not be produced.")
    return parsed_args


def exec_sql(dremio_environment_url, user, password, sql_filename, report_filename, log_level, log_filename):
    logger = Logger(level=log_level, log_file=log_filename)
    with open(sql_filename, 'r') as f:
        sql_code = f.read()
    sql_commands = sql_code.split(';')
    logger.new_process_status(len(sql_commands), 'Executing SQL. ')
    env_api = EnvApi(dremio_environment_url, user, password, logger)
    sql_statuses = []
    for sql in sql_commands:
        if sql:
            status, jobid, job_info = env_api.execute_sql(sql)
            job_result = env_api.get_job_result(jobid)
            sql_statuses.append({'sql': sql, 'jobid': jobid, 'job_info': job_info, 'job_result': job_result})
        logger.print_process_status(increment=1)
    # Produce execution report
    if report_filename:
        if os.path.isfile(report_filename):
            os.remove(report_filename)
        with open(report_filename, "w", encoding="utf-8") as f:
            json.dump(sql_statuses, f, indent=4, sort_keys=True)

    logger.finish_process_status_reporting()
    if logger.get_error_count() > 0:
        exit(Utils.NON_FATAL_EXIT_CODE)


if __name__ == '__main__':
    args = parse_args()
    exec_sql(args.dremio_environment_url, args.user, args.password, args.sql_filename,
             args.report_filename, args.log_level, args.log_filename)

