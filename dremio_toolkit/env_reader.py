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

from typing import Dict, Optional
from dremio_toolkit.env_api import EnvApi
from dremio_toolkit.env_definition import EnvDefinition
from dremio_toolkit.logger import Logger
from dremio_toolkit.utils import Utils
import time
import os

class ContainerType:
	HOME = "HOME"
	SPACE = "SPACE"
	SOURCE = "SOURCE"

	@classmethod
	def all(cls) -> list:
		return [cls.HOME, cls.SPACE, cls.SOURCE]


class EnvReader:
	_env_api = None
	_logger = None
	_env_def = EnvDefinition()

	def __init__(self, env_api: EnvApi, logger: Logger):
		self._env_api = env_api
		self._logger = logger
		self._env_def.endpoint = env_api.get_env_endpoint()
		# Current top-level hierarchy context: Home, Space, Source
		self._top_level_hierarchy_context: Optional[str] = None

	# Read all objects from the source Dremio environment and return as EnvDefinition
	def read_dremio_environment(self) -> EnvDefinition:
		self._read_catalogs()
		self._read_reflections()
		self._read_rules()
		self._read_queues()
		self._read_votes()
		return self._env_def

	def write_exception_report(self, report_file: str, delimiter: str = '\t') -> None:
		if report_file is None:
			self._logger.warn("Exception report file name has not been supplied with report-filename argument. Report file will not be produced.")
			return
		self._logger.new_process_status(100, 'Reporting Exceptions.')
		sql = "SELECT U.USER_NAME AS OWNER_USER_NAME, V.VIEW_NAME, V.PATH, V.SQL_DEFINITION, V.SQL_CONTEXT " \
			  "FROM SYS.\\\"VIEWS\\\" V " \
			  "JOIN SYS.\\\"USERS\\\" U ON V.OWNER_ID = U.USER_ID " \
			  "WHERE POSITION('@' IN PATH)=2 OR POSITION('@' IN SQL_CONTEXT)=1 "
		jobid = self._env_api.submit_sql(sql)
		# Wait for the job to complete. Should only take a moment
		while True:
			job_info = self._env_api.get_job_info(jobid)
			if job_info is None or job_info["jobState"] in ['CANCELED', 'FAILED']:
				self._logger.fatal("Unexpected error. Cannot get a list of INFORMATION_SCHEMA.TABLES.")
			if job_info["jobState"] == 'COMPLETED':
				break
			time.sleep(1)
		# Retrieve list of TABLES
		job_result = self._env_api.get_job_result(jobid)
		num_rows = int(job_result['rowCount'])
		if num_rows == 0:
			return
		# Prep report file
		if os.path.isfile(report_file):
			os.remove(report_file)
		with open(report_file, "w", encoding="utf-8") as f:
			f.write("OWNER_USER_NAME" + delimiter + "VIEW_NAME" + delimiter + "PATH" + delimiter + "NOTES\n")
			# Page through the results, 100 rows per page
			limit = 100
			for i in range(0, int(num_rows / limit) + 1):
				job_result = self._env_api.get_job_result(jobid, limit * i, limit)
				if job_result is not None:
					for row in job_result['rows']:
						f.write(row['OWNER_USER_NAME'] + delimiter + row['VIEW_NAME'] + delimiter +
								row['PATH'] + delimiter + "SQL_CONTEXT:" + row['SQL_CONTEXT'] + '\n')

	# Read top level Dremio catalogs from source Dremio environment,
	# traverse through the entire catalogs' hierarchies,
	# and save objects into self._env_api_def
	def _read_catalogs(self) -> None:
		containers = self._env_api.list_catalogs()['data']
		self._logger.new_process_status(len(containers), 'Reading Catalogs. ')
		for container in containers:
			container_type = container['containerType']
			if container_type in ContainerType.all():
				self._logger.debug(f"Processing {container_type} container: ", catalog=container)
				self._top_level_hierarchy_context = container_type

				if container not in self._env_def.containers:
					self._env_def.containers.append(container)

				self._read_entity(container, container_type)

			else:
				self._logger.fatal("Unexpected catalog type ", catalog=container)
			self._logger.print_process_status(increment=1)

	def _read_entity(self, container, container_type) -> None:
		entity = self._get_referenced_entity(container)
		if entity is not None:
			if container_type == ContainerType.HOME and entity not in self._env_def.homes:
				self._env_def.homes.append(entity)
			elif container_type == ContainerType.SPACE and entity not in self._env_def.spaces:
				self._env_def.spaces.append(entity)
			elif container_type == ContainerType.SOURCE and entity not in self._env_def.sources:
				self._env_def.sources.append(entity)

			self._read_entity_acl(entity)
			self._read_wiki(entity)

			if container_type in [ContainerType.HOME, ContainerType.SPACE]:
				self._read_space_or_folder_children(entity)

	# Read Space Folder container and traverse through its children hierarchy.
	def _read_space_folder_container(self, folder_container) -> None:
		self._logger.debug("Processing HOME/SPACE FOLDER: ", catalog=folder_container)
		if self._top_level_hierarchy_context not in [ContainerType.HOME, ContainerType.SPACE]:
			self._logger.error("Error, unexpected top level hierarchy while processing HOME/SPACE FOLDER: " + self._top_level_hierarchy_context)
			return
		folder_entity = self._get_referenced_entity(folder_container)
		if folder_entity is not None:
			if folder_entity not in self._env_def.folders:
				self._env_def.folders.append(folder_entity)
			self._read_entity_acl(folder_entity)
			self._read_wiki(folder_entity)
			self._read_space_or_folder_children(folder_entity)

	# Read Virtual Dataset container.
	def _read_virtual_dataset_container(self, dataset_container) -> None:
		self._logger.debug("Processing DATASET: ", catalog=dataset_container)
		dataset_entity = self._get_referenced_entity(dataset_container)
		if dataset_entity is not None:
			if dataset_container['datasetType'] == "PROMOTED" or dataset_container['datasetType'] == "DIRECT":
				self._logger.info(
					"Unexpected DATASET type: " + dataset_container['datasetType'] + " : ", catalog=dataset_container)
			elif dataset_container['datasetType'] == "VIRTUAL":
				if dataset_entity not in self._env_def.vds_list:
					self._env_def.vds_list.append(dataset_entity)
			else:
				self._logger.error(
					"Unexpected DATASET type " + dataset_container['datasetType'] + " for ", catalog=dataset_container)
			self._read_entity_acl(dataset_entity)
			self._read_wiki(dataset_entity)
			self._read_tags(dataset_entity)
			self._read_vds_graph(dataset_entity)

	# Read Home/Space/Folder container and traverse through its children hierarchy.
	def _read_space_or_folder_children(self, space) -> None:
		self._logger.debug("Processing children for HOME/SPACE/FOLDER: ", catalog=space)
		if 'entityType' not in space:
			self._logger.error("Unexpected json for HOME/SPACE/FOLDER: ", catalog=space)
			return
		for child in space['children']:
			if child['type'] == "DATASET":
				self._read_virtual_dataset_container(child)
			elif child['type'] == "FILE":
				self._read_file_container(child)
			elif child['containerType'] == "FOLDER":
				self._read_space_folder_container(child)
			else:
				self._logger.error("Unsupported catalog type " + child['type'])

	# Read File - can happen for uploaded user files. Ignore, but issue warning.
	def _read_file_container(self, file_container) -> None:
		self._logger.warn("Ignoring FILE: ", catalog=file_container)

	# Read All Reflections.
	def _read_reflections(self) -> list:
		reflections = self._env_api.list_reflections()['data']
		for reflection in reflections:
			reflection_dataset = self._env_api.get_catalog(reflection['datasetId'])
			if reflection_dataset is None:
				self._logger.error("Error processing reflection, cannot get path for dataset_container: " + reflection['datasetId'])
				continue
			reflection["path"] = reflection_dataset['path']
			if reflection not in self._env_def.reflections:
				self._env_def.reflections.append(reflection)

	# Read All Tags for a given catalog.
	def _read_tags(self, entity) -> None:
		self._logger.debug("Reading tags for catalog ", catalog=entity)
		tags = self._env_api.get_catalog_tags(entity['id'])
		if tags is not None:
			tags['entity_id'] = entity['id']
			if entity['entityType'] == 'space' or entity['entityType'] == 'source':
				tags['path'] = [entity['name']]
			else:
				tags['path'] = entity['path']
			if tags not in self._env_def.tags:
				self._env_def.tags.append(tags)

	# Read Wiki for a given catalog.
	def _read_wiki(self, entity) -> None:
		self._logger.debug("Reading wiki for catalog ", catalog=entity)
		wiki = self._env_api.get_catalog_wiki(entity['id'])
		if wiki is not None:
			wiki["entity_id"] = entity['id']
			if entity['entityType'] == 'space' or entity['entityType'] == 'source' or entity['entityType'] == 'home':
				wiki['path'] = [entity['name']]
			else:
				wiki['path'] = entity['path']
			if wiki not in self._env_def.wikis:
				self._env_def.wikis.append(wiki)

	def _read_queues(self) -> list:
		queues = self._env_api.list_queues()
		self._env_def.queues = queues['data'] if queues and 'data' in queues else []

	def _read_rules(self) -> list:
		rules = self._env_api.list_rules()
		self._env_def.rules = rules['rules'] if rules and 'rules' in rules else []

	# Read Votes.
	def _read_votes(self) -> list:
		votes = self._env_api.list_votes()
		self._env_def.votes = votes['data'] if votes and 'data' in votes else []

	# Reads Users, Groups, and Roles from entity AccessControlLost
	def _read_entity_acl(self, entity) -> None:
		# Read Owner
		if 'owner' in entity:
			owner_id = entity['owner']['ownerId']
			owner_type = entity['owner']['ownerType']
			if owner_type == 'USER':
				user_entity = self._env_api.get_user(owner_id)
				if user_entity is not None and user_entity not in self._env_def.referenced_users:
					self._env_def.referenced_users.append(user_entity)
			elif owner_type == 'GROUP':
				group_entity = self._env_api.get_group(owner_id)
				if group_entity is not None and group_entity not in self._env_def.referenced_groups:
					self._env_def.referenced_groups.append(group_entity)
			elif owner_type == 'ROLE':
				role_entity = self._env_api.get_role(owner_id)
				if role_entity is not None and role_entity not in self._env_def.referenced_roles:
					self._env_def.referenced_roles.append(role_entity)
			else:
				self._logger.error("Unexpected OwnerType '" + owner_type + "' for entity ", catalog=entity)
		# Read AccessControlList
		if 'accessControlList' in entity:
			acl = entity['accessControlList']
			if 'users' in acl:
				for user in acl['users']:
					user_entity = self._env_api.get_user(user['id'])
					if user_entity is not None and user_entity not in self._env_def.referenced_users:
						self._env_def.referenced_users.append(user_entity)
			if 'groups' in acl:
				for group in acl['groups']:
					group_entity = self._env_api.get_group(group['id'])
					if group_entity is not None and group_entity not in self._env_def.referenced_groups:
						self._env_def.referenced_groups.append(group_entity)
			if 'roles' in acl:
				for role in acl['roles']:
					role_entity = self._env_api.get_role(role['id'])
					if role_entity is not None and role_entity not in self._env_def.referenced_roles:
						self._env_def.referenced_roles.append(role_entity)

	# Helper method, used by many read* methods
	def _get_referenced_entity(self, ref) -> Optional[Dict]:
		self._logger.debug("Processing reference: ", catalog=ref)
		if 'id' not in ref:
			self._logger.error("Ref json does not contain catalog 'id', skipping: ", catalog=ref)
			return None
		else:
			return self._env_api.get_catalog(ref['id'])

	def _read_vds_graph(self, vds):
		graph = self._env_api.get_catalog_graph(vds['id'])
		if graph is not None:
			vds_parent_list = []
			for parent in graph['parents']:
				vds_parent_list.append(Utils.get_str_path(parent['path']))
			vds_parent_json = {'id': vds['id'], 'path': vds['path'], 'parents': vds_parent_list}
			self._env_def.vds_parents.append(vds_parent_json)
