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

import logging
from datetime import datetime

from dremio_toolkit.utils import Utils


class Logger:
    # Configuration
    _LEVELS = {'ERROR': 40, 'WARN': 30, 'WARNING': 30, 'INFO': 20, 'DEBUG': 10}

    def __init__(self, context, level=logging.ERROR, verbose=False, log_file: str = None):
        # Status print
        self._process_prefix_text = ''
        self._process_last_complete = 0
        self._process_total = 0
        self._process_start_time = None
        # Message collections
        self._errors = []
        # Other initialization
        self._context = context
        self._uuid = context.get_uuid()
        if type(level) == str:
            level = Logger._LEVELS[level]
        self._root_logger = logging.getLogger('root')
        self._root_logger.setLevel(level)
        self._error_count = 0
        self._verbose = verbose
        self._process_start_time = datetime.now()
        self._last_error_message = ''
        print('Running command ' + self._context.get_command() + '. Run ID: ' + self._uuid)
        if log_file:
            print('Logger will write to file: ' + log_file)
            logging.basicConfig(handlers=[logging.FileHandler(filename=log_file, encoding='utf-8', mode='a+')])

    def set_uuid(self, uuid: str):
        self._uuid = uuid

    def fatal(self, message: str, catalog: str = None) -> None:
        self._root_logger.critical(self._enrich_message(message, catalog))
        raise RuntimeError("Critical message: " + str(message))

    def get_last_error_message(self):
        return self._last_error_message

    def get_all_errors(self):
        return self._errors

    def error(self, message: str, catalog: str = None, object_list: list = None) -> None:
        self._last_error_message = message
        self._error_count += 1
        if object_list:
            enriched_message = self._enrich_message(message)
            if self._verbose:
                self.error(enriched_message + ' ' + str(object_list))
            else:
                self.error(enriched_message + ', total: ' + str(len(object_list)) + ' items.')
        else:
            enriched_message = self._enrich_message(message, catalog)
            self._root_logger.error(self._enrich_message(message, catalog))
        self._errors.append({'message': message, 'enriched_message': enriched_message, 'catalog': catalog,
                             'object_list': str(object_list)})

    def warn(self, message: str, catalog: str = None) -> None:
        self._root_logger.warning(self._enrich_message(message, catalog))

    def info(self, message: str, catalog: str = None) -> None:
        self._root_logger.info(self._enrich_message(message, catalog))

    def debug(self, message: str, catalog: str = None) -> None:
        self._root_logger.debug(self._enrich_message(message, catalog))

    def new_process_status(self, total: int, prefix_text='') -> None:
        self._process_prefix_text = prefix_text
        self._process_total = total
        self._process_last_complete = 0
        self._process_start_time = datetime.now()
        print()
        print(self._process_prefix_text, end='\r')

    def print_process_status(self, complete: int = None, increment: int = None) -> None:
        # Validate parameters
        if complete is None and increment is None:
            self.warn('Error reporting process status')
            return
        elif complete is not None:
            self._process_last_complete = complete
        else: # increment is not None
            complete = self._process_last_complete + increment
            self._process_last_complete = complete
        if complete != 0:
            pct_complete = complete / self._process_total * 100
            ttn = (datetime.now() - self._process_start_time)  // 1000000 * 1000000  # round to seconds
            etl = (ttn * (self._process_total / complete - 1)) // 1000000 * 1000000  # round to seconds
            if complete < self._process_total:
                print(self._process_prefix_text +
                      'Processed: ' + str(round(pct_complete)) + '% in ' + str(ttn) +
                      ' with ' + str(self._error_count) + ' errors.' +
                      ' Estimated time left: ' + str(etl) + '.', end='\r')
            else:
                print(self._process_prefix_text +
                      'Processed: ' + str(round(pct_complete)) + '% in ' + str(ttn) +
                      ' with ' + str(self._error_count) + ' errors.', end='\r')

    def finish_process_status_reporting(self) -> None:
        print()
        if self._error_count > 0:
            print("Process finished with " + str(self._error_count) +
                  " errors. Please review the log file for more information.")

    def get_error_count(self) -> int:
        return self._error_count

    # Enrich message with either catalog ID or entire catalog JSON depending on verbose setting
    def _enrich_message(self, message: str, catalog: dict = None) -> str:
        message = 'Run:' + self._uuid + ': ' + message
        if catalog is None or type(catalog) != dict:
            return message
        if self._verbose:
            return message + " " + str(catalog)
        if 'path' in catalog:
            if 'entityType' in catalog:
                return message + " " + str(catalog['entityType']) + ":" + Utils.get_str_path(catalog['path'])
            else:
                return message + " " + Utils.get_str_path(catalog['path'])
        if 'entityType' in catalog:
            if 'name' in catalog:
                return message + " " + str(catalog['entityType']) + ":" + str(catalog['name'])
            else:
                return message + " " + str(catalog['entityType']) + ":" + str(catalog['id'])
        return message + " " + str(catalog)
