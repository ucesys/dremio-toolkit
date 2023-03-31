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
import json
import os
from dremio_toolkit.logger import Logger
from dremio_toolkit.utils import Utils
from dremio_toolkit.env_api import EnvApi
from dremio_toolkit.env_definition import EnvDefinition


###
# This class uses EnvDefinition input object to update Dremio environment.
###
class EnvWriter:
    # Configuration
    _MAX_VDS_HIERARCHY_DEPTH = 10

    _logger = None
    _env_api = None
    _env_def = None

    # Objects that already present in the target Dremio environments
    _existing_dremio_users = []
    _existing_dremio_groups = []
    _existing_dremio_roles = []
    _existing_reflections = []

    # Lists used during VDS ordering
    _vds_hierarchy = []
    _referenced_pds = []

    # Lists of failed objects
    _failed_sources = []
    _failed_spaces = []
    _failed_folders = []
    _failed_reflections = []
    _failed_wiki = []
    _failed_tags = []

    def __init__(self, env_api: EnvApi, env_def: EnvDefinition, logger: Logger):
        self._env_api = env_api
        self._env_def = env_def
        self._logger = logger

    def write_dremio_environment(self) -> None:
        self._retrieve_referenced_acl_principals()
        self._read_existing_reflections()
        self._write_sources()
        self._write_spaces()
        self._write_space_folders()
        self._order_vds()
        self._resolve_referenced_pds()
        self._write_vds()
        self._write_reflections()
        self._write_wiki()
        self._write_tags()

    def write_exception_report(self, report_file: str, delimiter: str = '\t') -> None:
        if report_file is None:
            return
        # Prep report file
        if os.path.isfile(report_file):
            os.remove(report_file)
        with open(report_file, "w", encoding="utf-8") as f:
            f.write("ERROR" + delimiter + "OBJECT_TYPE" + delimiter + "ID" + delimiter + "PATH or NAME" +
                    delimiter + "NOTES" + "\n")
            for vds in self._env_def.vds_list:
                f.write('Unable to push' + delimiter + 'VDS' + delimiter + (vds['id'] if 'id' in vds else '') +
                        delimiter + str(vds['path']) + delimiter + '' + '\n')
            for vds in self._vds_hierarchy:
                f.write('Unable to push' + delimiter + 'VDS' + delimiter + (vds[1]['id'] if 'id' in vds[1] else '') +
                        delimiter + str(vds[1]['path']) + delimiter + 'Hierarchy Level: ' + str(vds[0]) + '\n')
            for source in self._failed_sources:
                f.write('Unable to push' + delimiter + 'SOURCE' + delimiter + (source['id'] if 'id' in source else '') +
                        delimiter + source['name'] + delimiter + '' + '\n')
            for space in self._failed_spaces:
                f.write('Unable to push' + delimiter + 'SPACE' + delimiter + (space['id'] if 'id' in space else '') +
                        delimiter + space['name'] + delimiter + '' + '\n')
            for folder in self._failed_folders:
                f.write('Unable to push' + delimiter + 'FOLDER' + delimiter + (folder['id'] if 'id' in folder else '') +
                        delimiter + str(folder['path']) + delimiter + '' + '\n')
            for wiki in self._failed_wiki:
                f.write('Unable to push' + delimiter + 'WIKI' + delimiter + (wiki['id'] if 'id' in wiki else '') +
                        delimiter + str(wiki['path']) + delimiter + '' + '\n')
            for tags in self._failed_tags:
                f.write('Unable to push' + delimiter + 'TAGS' + delimiter + (tags['id'] if 'id' in tags else '') +
                        delimiter + str(tags['path']) + delimiter + '' + '\n')

    def _retrieve_referenced_acl_principals(self) -> None:
        self._logger.new_process_status(3, 'Retrieving ACL Users. ')
        for user in self._env_def.referenced_users:
            existing_user = self._env_api.get_user_by_name(user['name'])
            if existing_user is not None:
                self._existing_dremio_users.append(existing_user)
            else:
                self._logger.warn("Unable to resolve user in target Dremio environment: ", user)
        self._logger.new_process_status(3, 'Retrieving ACL Groups. ')
        for group in self._env_def.referenced_groups:
            existing_group = self._env_api.get_group_by_name(group['name'])
            if existing_group is not None:
                self._existing_dremio_groups.append(existing_group)
            else:
                self._logger.warn("Unable to resolve group in target Dremio environment: ", group)
        self._logger.new_process_status(3, 'Retrieving ACL Roles. ')
        for role in self._env_def.referenced_roles:
            existing_role = self._env_api.get_role_by_name(role['name'])
            if existing_role is not None:
                self._existing_dremio_roles.append(existing_role)
            else:
                self._logger.error("Unable to resolve role in target Dremio environment: ", role)

    def _read_existing_reflections(self) -> None:
        self._logger.new_process_status(3, 'Retrieving Reflections. ')
        reflections = self._env_api.list_reflections()
        self._existing_reflections = reflections['data'] if reflections is not None else []

    def _write_sources(self) -> None:
        self._logger.new_process_status(len(self._env_def.sources), 'Pushing Sources. ')
        for source in self._env_def.sources:
            self._logger.print_process_status(increment=1)
            if not self._write_entity(source):
                self._failed_sources.append(source)

    def _write_spaces(self) -> None:
        self._logger.new_process_status(len(self._env_def.spaces), 'Pushing Spaces. ')
        for space in self._env_def.spaces:
            self._logger.print_process_status(increment=1)
            if not self._write_entity(space):
                self._failed_spaces.append(space)

    def _write_space_folders(self) -> None:
        self._logger.new_process_status(len(self._env_def.folders), 'Pushing Space Folders. ')
        for folder in self._env_def.folders:
            self._logger.print_process_status(increment=1)
            # Drop ACL for HOME folders
            if folder['path'][0][:1] == '@':
                Utils.pop_it(folder, ["accessControlList"])
            if not self._write_entity(folder):
                self._failed_folders.append(folder)

    # Process vds_list and save ordered list of VDSs into _vds_hierarchy. Recursive method.
    def _order_vds(self, processing_level=0):
        # Verify for the Max Hierarchy Depth
        if processing_level >= 10:
            self._logger.debug("Reached level depth limit while ordering VDSs. Still left to process:" +
                               str(self._env_def.vds_list))
            return
        self._logger.new_process_status(len(self._env_def.vds_list), 'Ordering VDS Hierarchy Level ' +
                                        str(processing_level) + '. ')
        any_vds_leveled = False
        # Iterate through the remainder VDS in the list
        # Go with decreasing index so we can remove VDS from the list
        vds_count = len(self._env_def.vds_list)
        for vds in reversed(self._env_def.vds_list):
            self._logger.print_process_status(increment=1)
            vds_hierarchy_level = processing_level
            sql_dependency_paths = self._get_vds_dependency_paths(vds)
            if sql_dependency_paths:
                for path in sql_dependency_paths:
                    sql_context = Utils.get_sql_context(vds)
                    dependency_vds = self._find_vds_by_path(Utils.get_absolute_path(path, sql_context))
                    if dependency_vds is None:
                        # Assume it's PDS since json file should have all VDS
                        Utils.append_unique_list(self._referenced_pds, Utils.get_absolute_path(path, sql_context))
                        continue
                    else:
                        # Dependency was found as VDS
                        dependency_hierarchy_level = self._find_vds_level_in_hierarchy(dependency_vds['id'])
                        if dependency_hierarchy_level is None:
                            # Dependency has not been processed yet, push this VDS to the next processing level
                            vds_hierarchy_level = None
                            break
                        # Find the highest level of hierarchy among dependencies
                        elif vds_hierarchy_level < dependency_hierarchy_level + 1:
                            vds_hierarchy_level = dependency_hierarchy_level + 1
            if vds_hierarchy_level is None:
                continue
            else:
                self._vds_hierarchy.append([vds_hierarchy_level, vds])
                self._env_def.vds_list.remove(vds)
                # Mark this hierarchy level as successful
                any_vds_leveled = True
        # Are we done yet with recursion
        if not any_vds_leveled or len(self._env_def.vds_list) == 0:
            return
        # Process the next Hierarchy Level recursively
        self._order_vds(processing_level + 1)

    def _resolve_referenced_pds(self):
        self._logger.new_process_status(len(self._referenced_pds), 'Resoving/Promoting Referenced PDS. ')
        for path in self._referenced_pds:
            pds = self._find_pds_by_path(path)
            if pds is None:
                # PDS could not be resolved.
                self._logger.error("Unable to resolve/promote PDS: " + path)
            self._logger.print_process_status(increment=1)

    def _write_vds(self) -> None:
        self._logger.new_process_status(len(self._vds_hierarchy), 'Pushing VDS Hierarchy. ')
        # First push all VDS that have been ordered into a hierarchy
        for level in range(0, 10, 1):
            for vds_hierarchy in reversed(self._vds_hierarchy):
                if level == vds_hierarchy[0]:
                    vds = vds_hierarchy[1]
                    if self._write_entity(vds):
                        self._vds_hierarchy.remove(vds_hierarchy)
                    self._logger.print_process_status(increment=1)
        # Iterate through the rest of VDS until all VDS have been successfully pushed to the target environment or
        # no VDS has been successfully pushed during the last iteration
        self._logger.new_process_status(len(self._vds_hierarchy), 'Pushing Unordered VDS. ')
        while self._env_def.vds_list:
            self._logger.print_process_status(increment=1)
            vds_updated = False
            for vds in reversed(self._env_def.vds_list):
                if self._write_entity(vds):
                    self._env_def.vds_list.remove(vds)
                    vds_updated = True
            if not vds_updated:
                break
        # Report on errors
        if self._vds_hierarchy:
            self._logger.error("Unable to push " + str(len(self._vds_hierarchy)) +
                               " VDSs from processed hierarchy. See exception report for details.")
        if self._env_def.vds_list:
            self._logger.error("Unable to push " + str(len(self._env_def.vds_list)) +
                               " un-ordered VDSs. See exception report for details.")

    def _write_entity(self, entity: dict) -> bool:
        # Prepare JSON object for saving to target Dremio environment
        # Clean up attributes that are automatically maintained by Dremio Environment
        Utils.pop_it(entity, ['id', 'tag', 'children', 'createdAt'])
        self._process_acl(entity)
        existing_entity = self._get_existing_entity(entity)
        if existing_entity is None:
            new_entity = self._env_api.create_catalog(entity)
            if new_entity is None:
                return False
        else:
            # Update entity id and concurrency tag with data from entity existing in the target environment
            entity['id'] = existing_entity['id']
            entity['tag'] = existing_entity['tag']
            # Update ACL version for proper concurrency control
            if ('path' in entity and entity['path'][0][:1] == '@') or ('name' in entity and entity['name'][:1] == '@'):
                Utils.pop_it(entity, 'accessControlList')
            else:
                if 'accessControlList' in existing_entity and 'version' in existing_entity['accessControlList']:
                    entity['accessControlList']['version'] = existing_entity['accessControlList']['version']
            updated_entity = self._env_api.update_catalog(existing_entity['id'], entity)
            if updated_entity is None:
                # Remove id from the entity for proper reporting
                Utils.pop_it(entity, ['id'])
                return False
        return True

    def _write_reflections(self) -> None:
        for reflection in self._env_def.reflections:
            reflection_path = reflection['path']
            Utils.pop_it(reflection, ['id', 'tag', 'createdAt', 'updatedAt', 'currentSizeBytes', 'totalSizeBytes', 'status', 'canView', 'canAlter', 'path'])
            reflected_dataset = self._env_api.get_catalog_by_path(Utils.get_str_path(reflection_path))
            if reflected_dataset is None:
                self._logger.error("Could not resolve reflected dataset for reflection: ", reflection)
                self._failed_reflections.append(reflection)
                continue
            reflection['datasetId'] = reflected_dataset['id']
            # Check if the reflection already exists
            existing_reflection = self._find_existing_reflection(reflection, reflected_dataset)
            if existing_reflection is None:
                new_reflection = self._env_api.create_reflection(reflection)
                if new_reflection is None:
                    self._logger.error("Could not create reflection ", reflection)
                    self._failed_reflections.append(reflection)
                    continue
            else:
                # Ensure there are changes to update as it will invalidate existing reflection data
                if self._is_reflection_equal(existing_reflection, reflection):
                    continue
                reflection['tag'] = existing_reflection['tag']
                updated_reflection = self._env_api.update_reflection(existing_reflection['id'], reflection)
                if updated_reflection is None:
                    self._logger.error("Error updating reflection ", reflection)
                    self._failed_reflections.append(reflection)
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
                self._failed_wiki.append(wiki)
                continue
            existing_wiki = self._env_api.get_catalog_wiki(existing_wiki_entity['id'])
            if existing_wiki is None:  # Need to create new entity
                new_wiki = {"text": wiki_text}
                new_wiki = self._env_api.update_wiki(existing_wiki_entity['id'], new_wiki)
                if new_wiki is None:
                    self._logger.error("Could not create wiki ", wiki)
                    self._failed_wiki.append(wiki)
                    continue
            else:  # Wiki already exists in the target environment
                if wiki_text == existing_wiki['text']:
                    continue
                existing_wiki['text'] = wiki_text
                updated_wiki = self._env_api.update_wiki(existing_wiki_entity['id'], existing_wiki)
                if updated_wiki is None:
                    self._logger.error("Error updating wiki ", wiki)
                    self._failed_wiki.append(wiki)
                    continue

    def _write_tags(self) -> None:
        for tags in self._env_def.tags:
            new_tags = tags['tags']
            tags_path = tags['path']
            # Check if the tags already exist
            existing_tags_entity = self._env_api.get_catalog_by_path(Utils.get_str_path(tags_path))
            if existing_tags_entity is None:
                self._logger.error("Unable to resolve dataset for tags ", tags)
                self._failed_tags.append(tags)
                continue
            existing_tags = self._env_api.get_catalog_tags(existing_tags_entity['id'])
            if existing_tags is None:
                new_tags = {"tags": new_tags}
                new_tags = self._env_api.update_tag(existing_tags_entity['id'], new_tags)
                if new_tags is None:
                    self._logger.error("Could not create tags ", tags)
                    self._failed_tags.append(tags)
                    continue
            else:
                if new_tags == existing_tags['tags']:
                    continue
                existing_tags['tags'] = new_tags
                updated_tags = self._env_api.update_tag(existing_tags_entity['id'], existing_tags)
                if updated_tags is None:
                    self._logger.error("Error updating tags ", tags)
                    self._failed_tags.append(tags)
                    continue

    def _get_vds_dependency_paths(self, vds):
        for vds_entry in self._env_def.vds_parents:
            if vds_entry['path'] == vds['path']:
                return vds_entry['parents']

    def _find_vds_by_path(self, path):
        # First, try finding in the VDS list from the source file
        for vds in self._env_def.vds_list:
            if path == Utils.get_str_path(vds['path']):
                return vds
        for vds_hierarchy in self._vds_hierarchy:
            if path == Utils.get_str_path(vds_hierarchy[1]['path']):
                return vds_hierarchy[1]
        return None

    def _find_pds_by_path(self, path):
        # Try finding in the target environment
        entity = self._env_api.get_catalog_by_path(path)
        # Ignore this condition as we can get folder instead for a valid PDS: Make sure we get promoted PDS and not folder/file
        if entity is None:
            return None
        elif Utils.is_pds(entity):
            return entity
        else:
            return self._env_api.promote_pds(entity)
        return None

    def _find_vds_level_in_hierarchy(self, vds_id):
        for item in self._vds_hierarchy:
            if item[1]['id'] == vds_id:
                return item[0]
        return None
