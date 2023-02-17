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


class ContainerType:
	HOME = "HOME"
	SPACE = "SPACE"
	SOURCE = "SOURCE"

	@classmethod
	def all(cls) -> list[str]:
		return [cls.HOME, cls.SPACE, cls.SOURCE]


class EnvReader:
	def __init__(self, dremio_env: EnvApi, dremio_tk_logger: Logger):
		self._dremio_env = dremio_env
		self._logger = dremio_tk_logger

		# Current top-level hierarchy context: Home, Space, Source
		self._top_level_hierarchy_context: Optional[str] = None

		self._containers: list[Dict] = []
		self._homes: list[Dict] = []
		self._spaces: list[Dict] = []
		self._sources: list[Dict] = []
		self._folders: list[Dict] = []
		self._vds_list: list[Dict] = []
		self._tags: list[Dict] = []
		self._wikis: list[Dict] = []
		self._referenced_users: list[Dict] = []
		self._referenced_groups: list[Dict] = []
		self._referenced_roles: list[Dict] = []

	# Read all objects from the source Dremio environment and return as EnvDefinition
	def read_dremio_environment(self) -> EnvDefinition:
		self._read_catalogs()

		return EnvDefinition(
			containers=self._containers,
			homes=self._homes,
			spaces=self._spaces,
			sources=self._sources,
			folders=self._folders,
			vds_list=self._vds_list,
			reflections=self._read_reflections(),
			rules=self._read_rules(),
			queues=self._read_queues(),
			votes=self._read_votes(),
			endpoint=self._dremio_env.get_env_endpoint(),
			tags=self._tags,
			wikis=self._wikis,
			referenced_users=self._referenced_users,
			referenced_groups=self._referenced_groups,
			referenced_roles=self._referenced_roles,
			files=[],
		)

	# Read top level Dremio catalogs from source Dremio environment,
	# traverse through the entire catalogs' hierarchies,
	# and save objects into self._dremio_env_def
	def _read_catalogs(self) -> None:
		containers = self._dremio_env.list_catalogs()['data']
		for container_id, container in enumerate(containers):
			container_type = container['containerType']
			if container_type in ContainerType.all():
				self._logger.debug(f"Processing {container_type} container: ", catalog=container)
				self._top_level_hierarchy_context = container_type

				if container not in self._containers:
					self._containers.append(container)

				self._read_entity(container, container_type)

			else:
				self._logger.fatal("Unexpected catalog type ", catalog=container)
			self._logger.print_process_status(len(containers), container_id+1)

	def _read_entity(self, container, container_type) -> None:
		entity = self._get_referenced_entity(container)
		if entity is not None:
			if container_type == ContainerType.HOME and entity not in self._homes:
				self._homes.append(entity)
			elif container_type == ContainerType.SPACE and entity not in self._spaces:
				self._spaces.append(entity)
			elif container_type == ContainerType.SOURCE and entity not in self._sources:
				self._sources.append(entity)

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
			if folder_entity not in self._folders:
				self._folders.append(folder_entity)
			self._read_entity_acl(folder_entity)
			self._read_wiki(folder_entity)
			self._read_space_or_folder_children(folder_entity)

	# Read Virtual Dataset container.
	def _read_virtual_dataset_container(self, dataset_container) -> None:
		self._logger.debug("Processing DATASET: ", catalog=dataset_container)
		dataset_entity = self._get_referenced_entity(dataset_container)
		if dataset_entity is not None:
			if dataset_container['datasetType'] == "PROMOTED" or dataset_container['datasetType'] == "DIRECT":
				self._logger.error(
					"Unexpected DATASET type: " + dataset_container['datasetType'] + " : ", catalog=dataset_container)
			elif dataset_container['datasetType'] == "VIRTUAL":
				if dataset_entity not in self._vds_list:
					self._vds_list.append(dataset_entity)
			else:
				self._logger.error(
					"Unexpected DATASET type " + dataset_container['datasetType'] + " for ", catalog=dataset_container)
			self._read_entity_acl(dataset_entity)
			self._read_wiki(dataset_entity)
			self._read_tags(dataset_entity)

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
	def _read_reflections(self) -> list[Dict]:
		self._logger.debug("Reading reflections ...")
		reflections = self._dremio_env.list_reflections()['data']
		reflections_with_path = []
		for reflection in reflections:
			reflection_dataset = self._dremio_env.get_catalog(reflection['datasetId'])
			if reflection_dataset is None:
				self._logger.debug("Error processing reflection, cannot get path for dataset_container: " + reflection['datasetId'])
				continue
			reflection["path"] = reflection_dataset['path']
			reflections_with_path.append(reflection)
		return reflections_with_path

	# Read All Tags for a given catalog.
	def _read_tags(self, entity) -> None:
		self._logger.debug("Reading tags for catalog ", catalog=entity)
		tags = self._dremio_env.get_catalog_tags(entity['id'])
		if tags is not None:
			tags['entity_id'] = entity['id']
			if entity['entityType'] == 'space' or entity['entityType'] == 'source':
				tags['path'] = [entity['name']]
			else:
				tags['path'] = entity['path']
			if tags not in self._tags:
				self._tags.append(tags)

	# Read Wiki for a given catalog.
	def _read_wiki(self, entity) -> None:
		self._logger.debug("Reading wiki for catalog ", catalog=entity)
		wiki = self._dremio_env.get_catalog_wiki(entity['id'])
		if wiki is not None:
			wiki["entity_id"] = entity['id']
			if entity['entityType'] == 'space' or entity['entityType'] == 'source' or entity['entityType'] == 'home':
				wiki['path'] = [entity['name']]
			else:
				wiki['path'] = entity['path']
			if wiki not in self._wikis:
				self._wikis.append(wiki)

	# Read WLM Queues.
	def _read_queues(self) -> list[Dict]:
		self._logger.debug("Reading WLM queues ...")
		queues = self._dremio_env.list_queues()
		return queues['data'] if queues and 'data' in queues else []

	# Read WLM Rules.
	def _read_rules(self) -> list[Dict]:
		self._logger.debug("Reading WLM rules ...")
		rules = self._dremio_env.list_rules()
		return rules['rules'] if rules and 'rules' in rules else []

	# Read Votes.
	def _read_votes(self) -> list[Dict]:
		self._logger.debug("Reading votes ...")
		votes = self._dremio_env.list_votes()
		return votes['data'] if votes and 'data' in votes else []

	# Reads Users, Groups, and Roles from entity AccessControlLost
	def _read_entity_acl(self, entity) -> None:
		# Read Owner
		if 'owner' in entity:
			owner_id = entity['owner']['ownerId']
			owner_type = entity['owner']['ownerType']
			if owner_type == 'USER':
				user_entity = self._dremio_env.get_user(owner_id)
				if user_entity is not None and user_entity not in self._referenced_users:
					self._referenced_users.append(user_entity)
			elif owner_type == 'GROUP':
				group_entity = self._dremio_env.get_group(owner_id)
				if group_entity is not None and group_entity not in self._referenced_groups:
					self._referenced_groups.append(group_entity)
			elif owner_type == 'ROLE':
				role_entity = self._dremio_env.get_role(owner_id)
				if role_entity is not None and role_entity not in self._referenced_roles:
					self._referenced_roles.append(role_entity)
			else:
				self._logger.error("Unexpected OwnerType '" + owner_type + "' for entity ", catalog=entity)
		# Read AccessControlList
		if 'accessControlList' in entity:
			acl = entity['accessControlList']
			if 'users' in acl:
				for user in acl['users']:
					user_entity = self._dremio_env.get_user(user['id'])
					if user_entity is not None and user_entity not in self._referenced_users:
						self._referenced_users.append(user_entity)
			if 'groups' in acl:
				for group in acl['groups']:
					group_entity = self._dremio_env.get_group(group['id'])
					if group_entity is not None and group_entity not in self._referenced_groups:
						self._referenced_groups.append(group_entity)
			if 'roles' in acl:
				for role in acl['roles']:
					role_entity = self._dremio_env.get_role(role['id'])
					if role_entity is not None and role_entity not in self._referenced_roles:
						self._referenced_roles.append(role_entity)

	# Helper method, used by many read* methods
	def _get_referenced_entity(self, ref) -> Optional[Dict]:
		self._logger.debug("Processing reference: ", catalog=ref)
		if 'id' not in ref:
			self._logger.error("Ref json does not contain catalog 'id', skipping: ", catalog=ref)
			return None
		else:
			return self._dremio_env.get_catalog(ref['id'])
