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

from dremio_toolkit.logger import Logger
from dremio_toolkit.utils import Utils
from dremio_toolkit.env_api import EnvApi
from dremio_toolkit.env_definition import EnvDefinition


###
# This class uses DremioData object to update Dremio environment.
###
class EnvWriter:
    # Configuration
    _MAX_VDS_HIERARCHY_DEPTH = 10

    _utils = Utils()
    _logger = None
    _env_api = None
    _env_def = None

    # Objects that already present in the target Dremio environments
    _existing_dremio_users = []
    _existing_dremio_groups = []
    _existing_dremio_roles = []
    _existing_reflections = []

    # Dry run collections
    _dry_run_processed_vds_list = []
    _dry_run_processed_pds_list = []

    def __init__(self, env_api: EnvApi, env_def: EnvDefinition, logger: Logger):
        self._env_api = env_api
        self._env_def = env_def
        self._logger = logger

    def write_dremio_environment(self) -> None:
        self._retrieve_referenced_acl_principals()
        self._logger.print_process_status(100, 1)
        self._read_existing_reflections()
        self._logger.print_process_status(100, 2)

        self._write_sources()
        self._logger.print_process_status(100, 3)
        self._write_spaces()
        self._logger.print_process_status(100, 4)
        self._write_space_folders()
        self._logger.print_process_status(100, 5)
        self._write_vds()
        self._write_reflections()
        self._write_wiki()
        self._write_tags()

    def _retrieve_referenced_acl_principals(self) -> None:
        for user in self._env_def.referenced_users:
            existing_user = self._env_api.get_user_by_name(user['name'])
            if existing_user is not None:
                self._existing_dremio_users.append(existing_user)
            else:
                self._logger.warn("Unable to resolve user in target Dremio environment: ", user)
        for group in self._env_def.referenced_groups:
            existing_group = self._env_api.get_group_by_name(group['name'])
            if existing_group is not None:
                self._existing_dremio_groups.append(existing_group)
            else:
                self._logger.warn("Unable to resolve group in target Dremio environment: ", group)
        for role in self._env_def.referenced_roles:
            existing_role = self._env_api.get_role_by_name(role['name'])
            if existing_role is not None:
                self._existing_dremio_roles.append(existing_role)
            else:
                self._logger.error("Unable to resolve role in target Dremio environment: ", role)

    def _read_existing_reflections(self) -> None:
        reflections = self._env_api.list_reflections()
        self._existing_reflections = reflections['data'] if reflections is not None else []

    def _write_sources(self) -> None:
        for source in self._env_def.sources:
            self._write_entity(source)

    def _write_spaces(self) -> None:
        for space in self._env_def.spaces:
            self._write_entity(space)

    def _write_space_folders(self) -> None:
        for folder in self._env_def.folders:
            # Drop ACL for HOME folders
            if folder['path'][0][:1] == '@':
                self._utils.pop_it(folder, "accessControlList")
            self._write_entity(folder)

    def _write_vds(self) -> None:
        # Iterate through VDS until all VDS have been successfully pushed to the target environment or
        # no VDS has been successfully pushed during the last iteration
        total_vds_count = len(self._env_def.vds_list)
        while self._env_def.vds_list:
            any_vds_pushed = False
            # These are VDSs that have all dependencies validated but could not be placed in the hierarchy
            for i in range(len(self._env_def.vds_list) - 1, -1, -1):
                vds = self._env_def.vds_list[i]
                if self._write_entity(vds):
                    self._env_def.vds_list.remove(vds)
                    any_vds_pushed = True
                self._logger.print_process_status(total_vds_count, total_vds_count-len(self._env_def.vds_list))
            if not any_vds_pushed:
                str_vds_list = str(self._env_def.vds_list)
                self._logger.error("Was not able to push the following VDS: " + str_vds_list)

    def _write_entity(self, entity: dict) -> bool:
        # Prepare JSON object for saving to target Dremio environment
        # Clean up attributes that are automatically maintained by Dremio Environment
        self._utils.pop_it(entity, ['id', 'tag', 'children', 'createdAt'])
        self._process_acl(entity)
        existing_entity = self._get_existing_entity(entity)
        if existing_entity is None:  # Need to create new entity
            if 'accessControlList' in entity:
                entity['accessControlList']['version'] = "0"
            new_entity = self._env_api.create_catalog(entity)
            if new_entity is None:
                return False
        else:
            # Update entity id and concurrency tag with data from entity existing in the target environment
            entity['id'] = existing_entity['id']
            entity['tag'] = existing_entity['tag']
            # Update ACL version for proper concurrency control
            if ('path' in entity and entity['path'][0][:1] == '@') or ('name' in entity and entity['name'][:1] == '@'):
                self._utils.pop_it(entity, 'accessControlList')
            else:
                if 'accessControlList' in existing_entity and 'version' in existing_entity['accessControlList']:
                    entity['accessControlList']['version'] = existing_entity['accessControlList']['version']
            updated_entity = self._env_api.update_catalog(entity['id'], entity)
            if updated_entity is None:
                return False
        return True

    def _write_reflections(self) -> None:
        for reflection in self._env_def.reflections:
            reflection_path = reflection['path']
            self._utils.pop_it(reflection, ['id', 'tag', 'createdAt', 'updatedAt', 'currentSizeBytes', 'totalSizeBytes', 'status', 'canView', 'canAlter', 'path'])
            reflected_dataset = self._env_api.get_catalog_by_path(Utils.get_str_path(reflection_path))
            if reflected_dataset is None:
                self._logger.error("Could not resolve reflected dataset for reflection: " + reflection)
                continue
            reflection['datasetId'] = reflected_dataset['id']
            # Check if the reflection already exists
            existing_reflection = self._find_existing_reflection(reflection, reflected_dataset)
            if existing_reflection is None:
                new_reflection = self._env_api.create_reflection(reflection)
                if new_reflection is None:
                    self._logger.error("Could not create reflection ", reflection)
                    continue
            else:
                # Ensure there are changes to update as it will invalidate existing reflection data
                if self._is_reflection_equal(existing_reflection, reflection):
                    continue
                reflection['tag'] = existing_reflection['tag']
                updated_reflection = self._env_api.update_reflection(existing_reflection['id'], reflection)
                if updated_reflection is None:
                    self._logger.error("Error updating reflection ", reflection)
                    continue

    def _is_reflection_equal(self, existing_reflection: dict, reflection: dict) -> bool:
        return reflection['type'] == existing_reflection['type'] and \
               reflection['name'] == existing_reflection['name'] and \
               (reflection.get('partitionDistributionStrategy') ==
                existing_reflection.get('partitionDistributionStrategy')) and \
               (reflection.get('measureFields') == existing_reflection.get('measureFields')) and \
               (reflection.get('dimensionFields') == existing_reflection.get('dimensionFields')) and \
               (reflection.get('displayFields') == existing_reflection.get('displayFields')) and \
               (reflection.get('sortFields') == existing_reflection.get('sortFields')) and \
               (reflection.get('partitionFields') == existing_reflection.get('partitionFields')) and \
               (reflection.get('distributionFields') == existing_reflection.get('distributionFields'))

    def _find_existing_reflection(self, reflection: dict, dataset: dict) -> dict:
        for existing_reflection in self._existing_reflections:
            if reflection['name'] == existing_reflection['name']:
                existing_dataset = self._env_api.get_catalog(existing_reflection['datasetId'])
                if existing_dataset is not None and existing_dataset['path'] == dataset['path']:
                    return existing_reflection
        return None

    # Search for Principals (users, groups, roles) from entity's ACL in the target environment and:
    # - update the ACL with principal id from the target environment if principal is found there
    # - return False or True to signify success
    def _process_acl(self, entity) -> None:
        if 'accessControlList' not in entity:
            return
        entity_acl = entity['accessControlList']
        new_acl = {"users": [], "groups": [], "roles": []}
        if 'users' in entity_acl:
            for user_def in entity_acl['users'][:]:
                acl_user = self._match_to_existing_user(user_def['id'])
                if acl_user is not None:
                    new_acl['users'].append({"id": acl_user["id"],
                                        "permissions": user_def['permissions'] if 'permissions' in user_def else []})
        if 'groups' in entity_acl:
            for group_def in entity_acl['groups'][:]:
                acl_group = self._match_to_existing_group(group_def['id'])
                if acl_group is not None:
                    new_acl['groups'].append({"id": acl_group["id"],
                                        "permissions": group_def['permissions'] if 'permissions' in group_def else []})
        if 'roles' in entity_acl:
            for role_def in entity_acl['roles'][:]:
                acl_role = self._match_to_existing_role(role_def['id'])
                if acl_role is not None:
                    new_acl['roles'].append({"id": acl_role["id"],
                                        "permissions": role_def['permissions'] if "permissions" in role_def else []})
        entity['accessControlList'] = new_acl

    def _match_to_existing_user(self, user_id: str) -> dict:
        for user in self._env_def.referenced_users:
            if user['id'] == user_id:
                for existing_user in self._existing_dremio_users:
                    if existing_user['name'] == user['name']:
                        return existing_user
        return None

    def _match_to_existing_group(self, group_id: str) -> dict:
        for group in self._env_def.referenced_groups:
            if group['id'] == group_id:
                for existing_group in self._existing_dremio_groups:
                    if existing_group['name'] == group['name']:
                        return existing_group
        return None

    def _match_to_existing_role(self, role_id: str) -> dict:
        for role in self._env_def.referenced_roles:
            if role['id'] == role_id:
                for existing_role in self._existing_dremio_roles:
                    if existing_role['name'] == role['name']:
                        return existing_role
        return None

    def _get_existing_entity(self, entity: dict) -> dict:
        if 'name' in entity:
            return self._env_api.get_catalog_by_path(entity['name'])
        elif 'path' in entity:
            return self._env_api.get_catalog_by_path(Utils.get_str_path(entity['path']))
        else:
            self._logger.error("Unable to find entity in the target Dremio environment: " + entity)
            return None

    def _write_wiki(self) -> None:
        for wiki in self._env_def.wikis:
            wiki_text = wiki['text']
            wiki_path = wiki['path']
            # Check if the wiki already exists
            existing_wiki_entity = self._env_api.get_catalog_by_path(Utils.get_str_path(wiki_path))
            if existing_wiki_entity is None:
                self._logger.error("Unable to resolve wiki's dataset for ", wiki)
                continue
            existing_wiki = self._env_api.get_catalog_wiki(existing_wiki_entity['id'])
            if existing_wiki is None:  # Need to create new entity
                new_wiki = {"text": wiki_text}
                new_wiki = self._env_api.update_wiki(existing_wiki_entity['id'], new_wiki)
                if new_wiki is None:
                    self._logger.error("Could not create wiki ", wiki)
                    continue
            else:  # Wiki already exists in the target environment
                if wiki_text == existing_wiki['text']:
                    continue
                existing_wiki['text'] = wiki_text
                updated_wiki = self._env_api.update_wiki(existing_wiki_entity['id'], existing_wiki)
                if updated_wiki is None:
                    self._logger.error("Error updating wiki ", wiki)
                    continue

    def _write_tags(self) -> None:
        for tags in self._env_def.tags:
            new_tags = tags['tags']
            tags_path = tags['path']
            # Check if the tags already exist
            existing_tags_entity = self._env_api.get_catalog_by_path(Utils.get_str_path(tags_path))
            if existing_tags_entity is None:
                self._logger.error("Unable to resolve dataset for tags ", tags)
                continue
            existing_tags = self._env_api.get_catalog_tags(existing_tags_entity['id'])
            if existing_tags is None:
                new_tags = {"tags": new_tags}
                new_tags = self._env_api.update_tag(existing_tags_entity['id'], new_tags)
                if new_tags is None:
                    self._logger.error("Could not create tags ", tags)
                    continue
            else:
                if new_tags == existing_tags['tags']:
                    continue
                existing_tags['tags'] = new_tags
                updated_tags = self._env_api.update_tag(existing_tags_entity['id'], existing_tags)
                if updated_tags is None:
                    self._logger.error("Error updating tags ", tags)
                    continue

