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
import time
import os
import json
from dremio_toolkit.utils import Utils
from dremio_toolkit.env_api import EnvApi
from dremio_toolkit.logger import Logger
from dremio_toolkit.rebuild_metadata_thread import RebuildMetadataThread
from dremio_toolkit.context import Context
import threading

MAX_ROWS_PER_PAGE = 100

def parse_args():
    # Process arguments
    arg_parser = argparse.ArgumentParser(
        description='rebuild_metadata is a part of the Dremio Toolkit. It forces Dremio to forget and then refresh metadata for all physical data sets as per supplied arguments.',
        epilog='Copyright UCE Systems Corp. For any assistance contact developer at dremio@ucesys.com'
    )
    arg_parser.add_argument("-d", "--dremio-environment-url", help="URL to Dremio environment.", required=True)
    arg_parser.add_argument("-u", "--user", help="User name. User must be a Dremio admin.", required=True)
    arg_parser.add_argument("-p", "--password", help="User password.", required=False)
    arg_parser.add_argument("-s", "--datasource", help="Limits the scope of the metadata refresh to physical datasets in a specified datasource. If not specified, metadata for all physical datasets in all datasources will be refreshed.", required=False)
    arg_parser.add_argument("-c", "--concurrency", help="Concurrency for executing metadata refresh. It is not recommended to set it higher than 4 if dremio.iceberg.enabled is not set to True. Default concurrency is 1.", required=False, default=1)
    arg_parser.add_argument("-r", "--report-filename", help="CSV file name for the JSON exception' report.", required=False)
    arg_parser.add_argument("-l", "--log-level", help="Set Log Level to DEBUG, INFO, WARN, ERROR.",
                            choices=['ERROR', 'WARN', 'INFO', 'DEBUG'], default='WARN')
    arg_parser.add_argument("-v", "--verbose", help="Set Log to verbose to print object definitions instead of object IDs.",
                            required=False, default=False, action='store_true')
    arg_parser.add_argument("-f", "--log-filename", help="Set Log to write to a specified file instead of STDOUT.",
                            required=False)
    parsed_args = arg_parser.parse_args()
    if parsed_args.report_filename is None:
        print("report-filename argument has not been specified. Exception report will not be produced.")
    if parsed_args.datasource is None:
        print("datasource argument has not been specified. Metadata for all physical datasets in all data sources will be rebuilt.")
    return parsed_args


def rebuild_metadata(ctx: Context, datasource, concurrency):
    logger = ctx.get_logger()
    logger.new_process_status(1, 'Retrieving list of PDS for rebuilding metadata ...')
    env_api = ctx.get_target_env_api()
    # Validate the Dremio environment can scale metadata refresh
    if concurrency > 4:
        iceberg_enabled, option_type, option_status = env_api.get_sys_option('dremio.iceberg.enabled', ctx.get_sql_comment_uuid())
        if not iceberg_enabled:
            print("Specifying concurrency higher than 4 without setting dremio.iceberg.enabled to True may impact Dremio performance and stability.")
    pds_list = get_pds_list(ctx, datasource)
    if pds_list is None:
        logger.fatal("Unable to retrieve list of PDS. ")
    if len(pds_list) == 0:
        print("\nNo PDS found in the specified scope. Nothing to do.")
    logger.new_process_status(len(pds_list), 'Rebuilding metadata. ')
    base_threads_count = threading.activeCount()
    threads = []
    for pds in reversed(pds_list):
        # Wait for a thread to become available to not exceed specified concurrency
        while threading.activeCount() >= base_threads_count + concurrency:
            time.sleep(3)
        new_thread = RebuildMetadataThread(ctx, pds)
        new_thread.start()
        threads.append(new_thread)

    # Wait for all threads to finish
    job_statuses = []
    for thread in threads:
        if thread.is_alive():
            thread.join()
        if thread.get_status():
            job_statuses.append({'pds': thread.get_pds_path(),
                                 'forget_job_id': thread.get_forget_job_id(),
                                 'refresh_job_id': thread.get_refresh_job_id(),
                                 'pds_rebuild_status': 'SUCCESS' if thread.get_status() else 'FAILED',
                                 'forget_job_info': thread.get_forget_job_info(),
                                 'refresh_job_info': thread.get_refresh_job_info()})

    # Produce execution report
    report_filename = ctx.get_report_filepath()
    if report_filename:
        if os.path.isfile(report_filename):
            os.remove(report_filename)
        with open(report_filename, "w", encoding="utf-8") as f:
            json.dump(job_statuses, f, indent=4, sort_keys=True)

    logger.finish_process_status_reporting()
    if logger.get_error_count() > 0:
        exit(Context.NON_FATAL_EXIT_CODE)


def get_pds_list(ctx: Context, datasource) -> list:
    sql = ctx.get_sql_comment_uuid() + \
          'SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA."TABLES" WHERE TABLE_TYPE = \'TABLE\''
    if datasource:
        sql += ' AND ( \'' + datasource.lower() + '\' = LOWER(TABLE_SCHEMA) OR ' +\
                       'POSITION(\'' + datasource.lower() + '.\' IN LOWER(TABLE_SCHEMA)) = 1)'
    env_api = ctx.get_target_env_api()
    status, jobid, job_result = env_api.execute_sql(sql)
    if not status:
        return None
    num_rows = int(job_result['rowCount'])
    # Page through the results, MAX_ROWS_PER_PAGE rows per page
    limit = MAX_ROWS_PER_PAGE
    pds_list = []
    for i in range(int(num_rows / limit) + 1):
        job_result = env_api.get_job_result(jobid, limit * i, limit)
        if job_result is not None:
            for row in job_result['rows']:
                table_fqn = '"' + row['TABLE_SCHEMA'].replace('.', '"."') + '"."' + row['TABLE_NAME'] + '"'
                pds_list.append(table_fqn)
    return pds_list


if __name__ == '__main__':
    args = parse_args()

    context = Context(Context.CMD_REBUILD_METADATA)
    context.init_logger(log_level=args.log_level, log_verbose=args.verbose, log_filepath=args.log_filename)
    context.set_target(env_api=EnvApi(args.dremio_environment_url, args.user, args.password, context))
    context.set_report(report_filepath=args.report_filename)

    rebuild_metadata(context, args.datasource, int(args.concurrency))

