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

import requests
import sys
import urllib


###
# DremioClient class provides a facade to Dremio API
#
###
class EnvApi:
    # Dremio RESTful API V2
    _api_v2 = 'apiv2'
    _login = _api_v2 + '/login'
    _eula = _api_v2 + '/eula/accept'
    # Dremio RESTful API V3
    _api_v3 = 'api/v3'
    _catalog = _api_v3 + '/catalog/'
    _reflection = _api_v3 + '/reflection/'
    _catalog_by_path = _api_v3 + '/catalog/by-path/'
    _wlm_queue = _api_v3 + "/wlm/queue/"
    _wlm_rule = _api_v3 + "/wlm/rule"
    _vote = _api_v3 + "/vote"
    _user = _api_v3 + "/user/"
    _user_by_name = _api_v3 + "/user/by-name/"
    _group = _api_v3 + "/group/"
    _group_by_name = _api_v3 + "/group/by-name/"
    _role = _api_v3 + "/role/"
    _role_by_name = _api_v3 + "/role/by-name/"
    _sql = _api_v3 + "/sql"
    _job = _api_v3 + "/job/"
    _graph_postfix = "graph"
    _refresh_postfix = "refresh"
    # Connection information
    _endpoint = ""
    _username = ""  # Need to keep for expired token processing
    _password = ""
    _token = ""
    # Configuration
    _verify_ssl = None
    DEFAULT_API_TIMEOUT = 120  # Accommodate for Dremio processing time
    _api_timeout: int = None
    _dry_run = None
    # Misc
    _timed_out_sources = []
    _headers = ""
    _logger = None

    def __init__(self, endpoint, username, password, dremio_tk_logger,
                 api_timeout=DEFAULT_API_TIMEOUT, verify_ssl=True, dry_run=True):
        self._logger = dremio_tk_logger
        self._endpoint = endpoint
        # Append slash to the endpoint url if needed
        if self._endpoint[-1:] != '/':
            self._endpoint += '/'
        self._username = username
        self._password = password
        self._verify_ssl = verify_ssl
        self._api_timeout = api_timeout
        self._dry_run = dry_run
        if not verify_ssl:
            self._logger.warn("Unverified SSL certificates will be accepted as per configuration.")
            requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)
        self._authenticate()

    # Return Dremio environment end point
    def get_env_endpoint(self):
        return self._endpoint

    # Generate an authentication token and save it
    # https://docs.dremio.com/software/rest-api/#authentication
    def _authenticate(self):
        headers = {"Content-Type": "application/json"}
        payload = '{"userName": "' + self._username + '","password": "' + self._password + '"}'
        payload = payload.encode(encoding='utf-8')
        response = requests.request("POST", self._endpoint + self._login, data=payload,
                                    headers=headers, timeout=self._api_timeout, verify=self._verify_ssl)
        if response.status_code != 200:
            self._logger.critical("Authentication Error " + str(response.status_code))
        self._token = '_dremio' + response.json()['token']
        self._headers = {"Content-Type": "application/json", "Authorization": self._token}

    # Lists all top-level catalog containers.
    # https://docs.dremio.com/software/rest-api/catalog/get-catalog/
    def list_catalogs(self):
        return self._http_get(self._catalog)

    # Returns a CatalogEntity by its path
    # https://docs.dremio.com/software/rest-api/catalog/get-catalog-path/
    def get_catalog_by_path(self, path):
        if path[0] == '/':
            path = path[1:]
        if '#' in path:
            path = path.replace("#", "%23")
        return self._http_get(self._catalog_by_path + path)

    # Returns a CatalogEntity by its ID
    # https://docs.dremio.com/software/rest-api/catalog/get-catalog-id/
    def get_catalog(self, catalog_id):
        # catalogId can be an actual Dremio UID or a path prefixed with 'dremio:'
        if catalog_id[:7] == 'dremio:':
            return self.get_catalog_by_path(catalog_id[8:])
        else:
            return self._http_get(self._catalog + catalog_id)

    # Retrieves graph information about a specific catalog entity
    # https://docs.dremio.com/software/rest-api/catalog/get-catalog-id-graph/
    def get_catalog_graph(self, catalog_id):
        return self._http_get(self._catalog + catalog_id + '/' + self._graph_postfix)

    # Returns the information of a user by username
    # https://docs.dremio.com/software/rest-api/user/get-user-2/
    def get_user_by_name(self, username):
        return self._http_get(self._user_by_name + username)

    # Returns the information of a user by id
    # https://docs.dremio.com/software/rest-api/user/get-user-2/
    def get_user(self, user_id):
        return self._http_get(self._user + user_id)

    # Returns the information of a group by name
    # https://docs.dremio.com/software/rest-api/accounts/get-group/
    def get_group_by_name(self, group_name):
        return self._http_get(self._group_by_name + group_name)

    # Returns the information of a group by id
    # https://docs.dremio.com/software/rest-api/accounts/get-group/
    def get_group(self, group_id):
        return self._http_get(self._group + group_id)

    # Returns the information of a role by name
    # https://docs.dremio.com/software/rest-api/roles/get-role-info/
    def get_role_by_name(self, role_name):
        return self._http_get(self._role_by_name + role_name)

    # Returns the information of a role by id
    # https://docs.dremio.com/software/rest-api/roles/get-role-info/
    def get_role(self, role_id):
        return self._http_get(self._role + role_id)

    # Returns a list of tags for a specified catalog ID
    # https: // docs.dremio.com / software / rest - api / catalog / wikis - tags /
    def get_catalog_tags(self, catalog_id):
        return self._http_get(self._catalog + catalog_id + "/collaboration/tag")

    # Returns wiki for a specified catalog ID
    # https://docs.dremio.com/software/rest-api/catalog/wikis-tags/
    def get_catalog_wiki(self, catalog_id):
        return self._http_get(self._catalog + catalog_id + "/collaboration/wiki")

    # Returns reflection definitions for a specified reflection ID
    # https://docs.dremio.com/software/rest-api/reflections/get-reflection-id/
    def get_reflection(self, reflection_id):
        return self._http_get(self._reflection + reflection_id)

    # Lists all reflections
    # https://docs.dremio.com/software/rest-api/reflections/get-reflection/
    def list_reflections(self):
        return self._http_get(self._reflection)

    # Lists all WLM Queues
    # https://docs.dremio.com/software/rest-api/wlm/get-wlm-queue/
    def list_queues(self):
        return self._http_get(self._wlm_queue)

    # Lists all WLM Rules
    # https://docs.dremio.com/software/rest-api/wlm/get-wlm-rule/
    def list_rules(self):
        return self._http_get(self._wlm_rule)

    # Lists all votes
    # https://docs.dremio.com/software/rest-api/votes/get-vote/
    def list_votes(self):
        return self._http_get(self._vote)

    # Create a catalog
    # https://docs.dremio.com/software/rest-api/catalog/post-catalog/
    def create_catalog(self, catalog_definition):
        if self._dry_run:
            self._logger.warning("Dry Run: not creating catalog.")
        else:
            return self._http_post(self._catalog, catalog_definition)

    # Updates existing datasets and sources
    # https://docs.dremio.com/software/rest-api/catalog/put-catalog-id/
    def update_catalog(self, catalog_id, catalog_definition):
        if self._dry_run:
            self._logger.warning("Dry Run: not updating catalog.")
        else:
            return self._http_put(self._catalog + catalog_id, catalog_definition)

    # Create a reflection
    # https://docs.dremio.com/software/rest-api/reflections/post-reflection/
    def create_reflection(self, reflection_definition):
        if self._dry_run:
            self._logger.warning("Dry Run: not creating reflection.")
        else:
            return self._http_post(self._reflection, reflection_definition)

    # Updates the specific reflection by the specified ID
    # https://docs.dremio.com/software/rest-api/reflections/put-reflection/
    def update_reflection(self, reflection_id, reflection_definition):
        if self._dry_run:
            self._logger.warning("Dry Run: not updating reflection.")
        else:
            return self._http_put(self._reflection + reflection_id, reflection_definition)

    # Refreshes all reflections dependent on a PDS specified by ID
    # https://docs.dremio.com/software/rest-api/catalog/post-catalog-id-refresh/
    def refresh_reflections_by_pds_id(self, pds_id):
        if self._dry_run:
            self._logger.warning("Dry Run: not refreshing reflections by PDS ID.")
        else:
            return self._http_post(self._catalog + pds_id + "/" + self._refresh_postfix)

    # Refreshes all reflections dependent on a PDS specified by path
    def refresh_reflections_by_pds_path(self, pds_path):
        pds = self.get_catalog_by_path(pds_path)
        if pds is None:
            self._logger.error("Could not locate PDS for path: " + str(pds_path))
            return None
        if self._dry_run:
            self._logger.warning("Dry Run: not refreshing reflections by PDS PATH.")
            return
        pds_id = pds['id']
        return self._http_post(self._catalog + pds_id + "/" + self._refresh_postfix)

    # Updates wiki for a catalog specified by ID
    # https://docs.dremio.com/software/rest-api/catalog/post-catalog-collaboration/
    def update_wiki(self, catalog_id, wiki):
        if self._dry_run:
            self._logger.warning("Dry Run: not updating wiki.")
        else:
            return self._http_post(self._catalog + catalog_id + "/collaboration/wiki", wiki)

    # Updates tag for a catalog specified by ID
    # https://docs.dremio.com/software/rest-api/catalog/post-catalog-collaboration/
    def update_tag(self, catalog_id, tag):
        if self._dry_run:
            self._logger.warning("Dry Run: not updating tag.")
        else:
            return self._http_post(self._catalog + catalog_id + "/collaboration/tag", tag)

    # Promotes a file or folder in a file-based source to a physical dataset
    # https://docs.dremio.com/software/rest-api/catalog/post-catalog-id/
    def promote_pds(self, pds):
        if self._dry_run:
            self._logger.warning("Dry Run: not promoting PDS.")
            return
        return self._http_post(self._catalog + self._encode_http_param(pds['id']), pds)

    # Submits SQL for execution and returns Job ID or None. It does not wait for the query to execute.
    # https://docs.dremio.com/software/rest-api/sql/post-sql/
    def submit_sql(self, sql, sql_context=None):
        payload = '{ "sql":"' + sql + '"' + ("" if sql_context is None else ', "context":"' + sql_context + '"') + ' }'
        result = self._http_post(self._sql, payload, as_json=False)
        if result is not None:
            return result["id"]
        else:
            return None

    # Returns information for a job specified by ID
    # https://docs.dremio.com/software/rest-api/job/
    def get_job_info(self, jobid):
        return self._http_get(self._job + jobid)

    # Returns job results for a job specified by ID
    # https://docs.dremio.com/software/rest-api/job/job-results/
    def get_job_result(self, jobid, offset=0, limit=100):
        return self._http_get(self._job + jobid + '/results?offset=' + str(offset) + '&limit=' + str(limit))

    # Deletes a reflection specified by ID
    # https://docs.dremio.com/software/rest-api/reflections/delete-reflection/
    def delete_reflection(self, reflection_id):
        if self._dry_run:
            self._logger.warning("Dry Run: not deleting reflection.")
        else:
            return self._http_delete(self._reflection + reflection_id)

    # Deletes a catalog specified by ID
    # https://docs.dremio.com/software/rest-api/catalog/delete-catalog-id/
    def delete_catalog(self, entity_id):
        if self._dry_run:
            self._logger.warning("Dry Run: not deleting catalog.")
        else:
            return self._http_delete(self._catalog + entity_id)

    # Executes HTTP GET. Returns JSON if success or None
    def _http_get(self, url, re_authenticate=False):
        if re_authenticate:
            self._authenticate()
        # Infer source
        source_name = None
        pos = url.find(self._catalog_by_path)
        if pos >= 0:
            source_name = url[pos + 23:]
            source_name = source_name[0:source_name.find("/")]
        else:
            pos = url.find(self._catalog)
            if pos >= 0:
                source_name = url[pos + 23:]
                source_name = source_name[0:source_name.find("/")]

        try:
            if source_name in self._timed_out_sources:
                raise requests.exceptions.Timeout()
            response = requests.request("GET", self._endpoint + url, headers=self._headers, timeout=self._api_timeout,
                                        verify=self._verify_ssl)
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 400:  # Bad Request
                self._logger.info("Received HTTP Response Code " + str(response.status_code) +
                                  " for : <" + str(url) + ">" + self._get_error_message(response))
            elif response.status_code == 404:  # Not found
                self._logger.info("Received HTTP Response Code " + str(response.status_code) +
                                  " for : <" + str(url) + ">" + self._get_error_message(response))
            elif response.status_code == 401 or response.status_code == 403:
                # Try to re-authenticate once since the token might expire
                if not re_authenticate:
                    return self._http_get(url, True)
                self._logger.critical("Received HTTP Response Code " + str(response.status_code) +
                                      " for : <" + str(url) + ">" + self._get_error_message(response))
                raise RuntimeError(self._get_error_message(response))
            else:
                self._logger.error("Received HTTP Response Code " + str(response.status_code) +
                                   " for : <" + str(url) + ">" + self._get_error_message(response))
            return None
        except requests.exceptions.Timeout:
            if source_name is None or source_name not in self._timed_out_sources:
                # This situation might happen when an underlying object (file system eg) is not responding
                self._logger.error("HTTP Request Timed-out: " + " <" + str(url) + ">")
            if source_name is not None and source_name not in self._timed_out_sources:
                self._timed_out_sources.append(source_name)
            return None

    # Executes HTTP POST. Returns JSON if success or None
    def _http_post(self, url, json_data=None, as_json=True, re_authenticate=False):
        if re_authenticate:
            self._authenticate()
        try:
            try:
                if json_data and isinstance(json_data, str):
                    json_data = json_data.encode("utf-8")
            except UnicodeEncodeError as e:
                self._logger.error(e)
                self._logger.error(f"Data: {json_data}")
            if json_data is None:
                response = requests.request("POST", self._endpoint + url, headers=self._headers,
                                            timeout=self._api_timeout, verify=self._verify_ssl)
            elif as_json:
                response = requests.request("POST", self._endpoint + url, json=json_data, headers=self._headers,
                                            timeout=self._api_timeout, verify=self._verify_ssl)
            else:
                response = requests.request("POST", self._endpoint + url, data=json_data, headers=self._headers,
                                            timeout=self._api_timeout, verify=self._verify_ssl)
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 204:
                return None
            elif response.status_code == 400:
                self._logger.error("Received HTTP Response Code " + str(response.status_code) +
                                   " for : <" + str(url) + ">" + self._get_error_message(response))
            elif response.status_code == 401 or response.status_code == 403:
                # Try to re-authenticate since the token might expire
                if not re_authenticate:
                    return self._http_post(url, json_data, as_json, False)
                self._logger.critical("Received HTTP Response Code " + str(response.status_code) +
                                      " for : <" + str(url) + ">" + self._get_error_message(response))
                raise RuntimeError(self._get_error_message(response))
            elif response.status_code == 409:  # Already exists.
                self._logger.error("Received HTTP Response Code " + str(response.status_code) +
                                   " for : <" + str(url) + ">" + self._get_error_message(response))
            elif response.status_code == 404:  # Not found
                self._logger.info("Received HTTP Response Code " + str(response.status_code) +
                                  " for : <" + str(url) + ">" + self._get_error_message(response))
            else:
                self._logger.error("Received HTTP Response Code " + str(response.status_code) +
                                   " for : <" + str(url) + ">" + self._get_error_message(response))
            return None
        except requests.exceptions.Timeout:
            # This situation might happen when an underlying object (file system eg) is not responding
            self._logger.error("HTTP Request Timed-out: " + " <" + str(url) + ">")
            return None

    # Executes HTTP PUT. Returns JSON if success or None
    def _http_put(self, url, json_data, re_authenticate=False):
        if re_authenticate:
            self._authenticate()
        try:
            response = requests.request("PUT", self._endpoint + url, json=json_data, headers=self._headers,
                                        timeout=self._api_timeout, verify=self._verify_ssl)
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 400:  # The supplied CatalogEntity object is invalid.
                self._logger.error("Received HTTP Response Code 400 for : <" + str(url) + ">" +
                                   self._get_error_message(response))
            elif response.status_code == 401 or response.status_code == 403:
                # Try to re-authenticate since the token might expire
                if not re_authenticate:
                    return self._http_put(url, json_data, False)
                self._logger.critical("Received HTTP Response Code " + str(response.status_code) +
                                      " for : <" + str(url) + ">" + self._get_error_message(response))
                raise RuntimeError(self._get_error_message(response))
            elif response.status_code == 409:  # A catalog catalog with the specified path already exists.
                self._logger.error("Received HTTP Response Code 409 for : <" + str(url) + ">" +
                                   self._get_error_message(response))
            elif response.status_code == 404:  # Not found
                self._logger.info("Received HTTP Response Code 404 for : <" + str(url) + ">" +
                                  self._get_error_message(response))
            else:
                self._logger.error("Received HTTP Response Code " + str(response.status_code) +
                                   " for : <" + str(url) + ">" + self._get_error_message(response))
            return None
        except requests.exceptions.Timeout:
            # This situation might happen when an underlying object (file system eg) is not responding
            self._logger.error("HTTP Request Timed-out: " + " <" + str(url) + ">")
            return None

    # Executes HTTP DELETE. Returns JSON if success or None
    def _http_delete(self, url, re_authenticate=False):
        if re_authenticate:
            self._authenticate()
        try:
            response = requests.request("DELETE", self._endpoint + url, headers=self._headers,
                                        timeout=self._api_timeout, verify=self._verify_ssl)
            if response.status_code == 200:
                if response.text == '':
                    # if text is empty then response.json() fails, e.g. delete reflections return 200 and empty text.
                    return None
                return response.json()
            elif response.status_code == 204:
                return None
            elif response.status_code == 400:  # The supplied CatalogEntity object is invalid.
                self._logger.error("Received HTTP Response Code 400 for : <" + str(url) + ">" +
                                   self._get_error_message(response))
            elif response.status_code == 401 or response.status_code == 403:
                # Try to re-authenticate since the token might expire
                if not re_authenticate:
                    return self._http_delete(url, False)
                self._logger.critical("Received HTTP Response Code " + str(response.status_code) +
                                      " for : <" + str(url) + ">" + self._get_error_message(response))
                raise RuntimeError(self._get_error_message(response))
            elif response.status_code == 409:  # A catalog catalog with the specified path already exists.
                self._logger.error("Received HTTP Response Code 409 for : <" + str(url) + ">" +
                                   self._get_error_message(response))
            elif response.status_code == 404:  # Not found
                self._logger.info("Received HTTP Response Code 404 for : <" + str(url) + ">" +
                                  self._get_error_message(response))
            else:
                self._logger.error("Received HTTP Response Code " + str(response.status_code) +
                                   " for : <" + str(url) + ">" + self._get_error_message(response))
            return None
        except requests.exceptions.Timeout:
            # This situation might happen when an underlying object (file system eg) is not responding
            self._logger.error("HTTP Request Timed-out: " + " <" + str(url) + ">")
            return None

    def _get_error_message(self, response):
        message = ""
        try:
            if 'errorMessage' in response.json():
                message = message + " errorMessage: " + str(response.json()['errorMessage'])
            if 'moreInfo' in response.json():
                message = message + " moreInfo: " + str(response.json()['moreInfo'])
        except:
            message = message + " content: " + str(response.content)
        return message

    def _encode_http_param(self, path):
        if sys.version_info.major > 2:
            # handle spaces in non-promoted source or folder paths (want %20 for Dremio URLs rather than +)
            return urllib.parse.quote_plus(urllib.parse.quote(path), safe='%')
        else:
            return urllib.quote_plus(path)
