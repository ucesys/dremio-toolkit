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

from .env_api import EnvApi
from .env_definition import EnvDefinition
from .logger import Logger
from .utils import Utils


class EnvReader:

	_logger: Logger = None
	_utils = Utils()
	_dremio_env: EnvApi = None
	_dremio_env_def = EnvDefinition()

	# Current top-level hierarchy context: Home, Space, Source
	_top_level_hierarchy_context: str = None

	def __init__(self, dremio_env, dremio_tk_logger):
		self._dremio_env = dremio_env
		self._logger = dremio_tk_logger

	# Read all objects from the source Dremio environment and return as EnvDefinition
	def read_dremio_environment(self) -> EnvDefinition:
		self._read_catalogs()
		self._read_reflections()
		self._read_rules()
		self._read_queues()
		self._read_votes()
		return self._dremio_env_def

	# Read top level Dremio catalogs from source Dremio environment,
	# traverse through the entire catalogs' hierarchies,
	# and save objects into self._dremio_env_def
	def _read_catalogs(self):
		containers = self._dremio_env.list_catalogs()['data']
		complete = 0
		for container in containers:
			self._logger.print_process_status(len(containers), complete)
			if container['containerType'] == "HOME":
				# Note, it processes only one Home container that belongs to the authenticated User.
				self._read_home_container(container)
			elif container['containerType'] == "SPACE":
				self._read_space_container(container)
			elif container['containerType'] == "SOURCE":
				self._read_source_container(container)
			else:
				self._logger.fatal("Unexpected catalog type ", catalog=container)
			complete += 1
		self._logger.print_process_status(len(containers), complete)

	# Read Home container with traversing through its children hierarchy.
	def _read_home_container(self, home_container):
		self._logger.debug("Processing HOME container: ", catalog=home_container)
		self._top_level_hierarchy_context = "HOME"
		self._utils.append_unique_list(self._dremio_env_def.containers, home_container)
		home_entity = self._get_referenced_entity(home_container)
		if home_entity is not None:
			self._utils.append_unique_list(self._dremio_env_def.homes, home_entity)
			self._read_entity_acl(home_entity)
			self._read_wiki(home_entity)
			self._read_space_or_folder_children(home_entity)

	# Read Space container and traverse through its children hierarchy.
	def _read_space_container(self, space_container):
		self._logger.debug("Processing SPACE container: ", catalog=space_container)
		self._top_level_hierarchy_context = "SPACE"
		self._utils.append_unique_list(self._dremio_env_def.containers, space_container)
		space_entity = self._get_referenced_entity(space_container)
		if space_entity is not None:
			self._utils.append_unique_list(self._dremio_env_def.spaces, space_entity)
			self._read_entity_acl(space_entity)
			self._read_wiki(space_entity)
			self._read_space_or_folder_children(space_entity)

	# Read Source container. DO NOT traverse through its children hierarchy.
	def _read_source_container(self, source_container):
		self._logger.debug("Processing SOURCE container: ", catalog=source_container)
		self._top_level_hierarchy_context = "SOURCE"
		self._utils.append_unique_list(self._dremio_env_def.containers, source_container)
		source_entity = self._get_referenced_entity(source_container)
		if source_entity is not None:
			self._utils.append_unique_list(self._dremio_env_def.sources, source_entity)
			self._read_entity_acl(source_entity)
			self._read_wiki(source_entity)

	# Read Space Folder container and traverse through its children hierarchy.
	def _read_space_folder_container(self, folder_container):
		self._logger.debug("Processing HOME/SPACE FOLDER: ", catalog=folder_container)
		if self._top_level_hierarchy_context not in ["SPACE", "HOME"]:
			self._logger.error("Error, unexpected top level hierarchy while processing HOME/SPACE FOLDER: " + self._top_level_hierarchy_context)
			return
		folder_entity = self._get_referenced_entity(folder_container)
		if folder_entity is None:
			return
		self._utils.append_unique_list(self._dremio_env_def.folders, folder_entity)
		self._read_entity_acl(folder_entity)
		self._read_wiki(folder_entity)
		self._read_space_or_folder_children(folder_entity)

	# Read Virtual Dataset container.
	def _read_virtual_dataset_container(self, dataset_container):
		self._logger.debug("Processing DATASET: ", catalog=dataset_container)
		dataset_entity = self._get_referenced_entity(dataset_container)
		if dataset_entity is not None:
			if dataset_container['datasetType'] == "PROMOTED" or dataset_container['datasetType'] == "DIRECT":
				self._logger.error(
					"Unexpected DATASET type: " + dataset_container['datasetType'] + " : ", catalog=dataset_container)
			elif dataset_container['datasetType'] == "VIRTUAL":
				# tags = self._dremio_env.get_catalog_tags(catalog['id'])
				self._utils.append_unique_list(self._dremio_env_def.vds_list, dataset_entity)
			else:
				self._logger.error(
					"Unexpected DATASET type " + dataset_container['datasetType'] + " for ", catalog=dataset_container)
			self._read_entity_acl(dataset_entity)
			self._read_wiki(dataset_entity)
			self._read_tags(dataset_entity)

	# Read Home/Space/Folder container and traverse through its children hierarchy.
	def _read_space_or_folder_children(self, space):
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
	def _read_file_container(self, file_container):
		self._logger.warn("Ignoring FILE: ", catalog=file_container)

	# Read All Reflections.
	def _read_reflections(self):
		self._logger.debug("Reading reflections ...")
		reflections = self._dremio_env.list_reflections()['data']
		for reflection in reflections:
			reflection_dataset = self._dremio_env.get_catalog(reflection['datasetId'])
			if reflection_dataset is None:
				self._logger.debug("Error processing reflection, cannot get path for dataset_container: " + reflection['datasetId'])
				continue
			reflection["path"] = reflection_dataset['path']
			self._utils.append_unique_list(self._dremio_env_def.reflections, reflection)

	# Read All Tags for a given catalog.
	def _read_tags(self, entity):
		self._logger.debug("Reading tags for catalog ", catalog=entity)
		tags = self._dremio_env.get_catalog_tags(entity['id'])
		if tags is not None:
			tags['entity_id'] = entity['id']
			if entity['entityType'] == 'space' or entity['entityType'] == 'source':
				tags['path'] = [entity['name']]
			else:
				tags['path'] = entity['path']
			self._utils.append_unique_list(self._dremio_env_def.tags, tags)

	# Read Wiki for a given catalog.
	def _read_wiki(self, entity):
		self._logger.debug("Reading wiki for catalog ", catalog=entity)
		wiki = self._dremio_env.get_catalog_wiki(entity['id'])
		if wiki is not None:
			wiki["entity_id"] = entity['id']
			if entity['entityType'] == 'space' or entity['entityType'] == 'source' or entity['entityType'] == 'home':
				wiki['path'] = [entity['name']]
			else:
				wiki['path'] = entity['path']
			self._utils.append_unique_list(self._dremio_env_def.wikis, wiki)

	# Read WLM Queues.
	def _read_queues(self):
		self._logger.debug("Reading WLM queues ...")
		list_queues = self._dremio_env.list_queues()
		if list_queues and 'data' in list_queues:
			self._dremio_env_def.queues = list_queues['data']
		else:
			self._dremio_env_def.queues = []

	# Read WLM Rules.
	def _read_rules(self):
		self._logger.debug("Reading WLM rules ...")
		list_rules = self._dremio_env.list_rules()
		if list_rules and 'rules' in list_rules:
			self._dremio_env_def.rules = list_rules['rules']
		else:
			self._dremio_env_def.rules = []

	# Read Votes.
	def _read_votes(self):
		self._logger.debug("Reading votes ...")
		list_votes = self._dremio_env.list_votes()
		if list_votes and 'data' in list_votes:
			self._dremio_env_def.votes = list_votes['data']
		else:
			self._dremio_env_def.votes = []

	# Reads Users, Groups, and Roles from entity AccessControlLost
	def _read_entity_acl(self, entity):
		# Read Owner
		if 'owner' in entity:
			owner_id = entity['owner']['ownerId']
			owner_type = entity['owner']['ownerType']
			if owner_type == 'USER':
				user_entity = self._dremio_env.get_user(owner_id)
				if user_entity is not None:
					self._utils.append_unique_list(self._dremio_env_def.referenced_users, user_entity)
			elif owner_type == 'GROUP':
				group_entity = self._dremio_env.get_group(owner_id)
				if group_entity is not None:
					self._utils.append_unique_list(self._dremio_env_def.referenced_groups, group_entity)
			elif owner_type == 'ROLE':
				role_entity = self._dremio_env.get_role(owner_id)
				if role_entity is not None:
					self._utils.append_unique_list(self._dremio_env_def.referenced_roles, role_entity)
			else:
				self._logger.error("Unexpected OwnerType '" + owner_type + "' for entity ", catalog=entity)
		# Read AccessControlList
		if 'accessControlList' in entity:
			acl = entity['accessControlList']
			if 'users' in acl:
				for user in acl['users']:
					user_entity = self._dremio_env.get_user(user['id'])
					if user_entity is not None:
						self._utils.append_unique_list(self._dremio_env_def.referenced_users, user_entity)
			if 'groups' in acl:
				for group in acl['groups']:
					group_entity = self._dremio_env.get_group(group['id'])
					if group_entity is not None:
						self._utils.append_unique_list(self._dremio_env_def.referenced_groups, group_entity)
			if 'roles' in acl:
				for role in acl['roles']:
					role_entity = self._dremio_env.get_role(role['id'])
					if role_entity is not None:
						self._utils.append_unique_list(self._dremio_env_def.referenced_roles, role_entity)

	# Helper method, used by many read* methods
	def _get_referenced_entity(self, ref):
		self._logger.debug("Processing reference: ", catalog=ref)
		if 'id' not in ref:
			self._logger.error("Ref json does not contain catalog 'id', skipping: ", catalog=ref)
			return None
		else:
			entity = self._dremio_env.get_catalog(ref['id'])
			return entity
