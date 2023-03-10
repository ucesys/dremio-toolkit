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
from dremio_toolkit.env_definition import EnvDefinition
import os


class DiffType:
    MATCH = 0
    DIFF_UID = 1
    DIFF_ATTRIBUTES = 2
    MISSING_ATTRIBUTE = 3
    DIFF_PERMISSIONS = 4
    DIFF_OWNER = 5

    @classmethod
    def all(cls) -> list:
        return [cls.MATCH, cls.DIFF_UID, cls.DIFF_ATTRIBUTES]


class EnvDiff:
    diff_containers = []
    diff_sources = []
    diff_spaces = []
    diff_folders = []
    diff_vds = []
    diff_reflections = []
    diff_rules = []
    diff_queues = []
    diff_tags = []
    diff_wikis = []

    _base_def: EnvDefinition
    _comp_def: EnvDefinition

    def __init__(self, logger: Logger):
        self._logger = logger

    def diff_snapshot(self, base_env_def: EnvDefinition, comp_env_def: EnvDefinition) -> None:
        self._base_def = base_env_def
        self._comp_def = comp_env_def
        self._logger.new_process_status(100,'Comparing snapshots. ')
        self._diff_containers()
        self._logger.print_process_status(complete=5)
        self._diff_sources()
        self._logger.print_process_status(complete=10)
        self._diff_spaces()
        self._logger.print_process_status(complete=15)
        self._diff_folders()
        self._logger.print_process_status(complete=30)
        self._diff_vds()
        self._logger.print_process_status(complete=80)
        self._diff_reflections()
        self._logger.print_process_status(complete=85)
        self._diff_rules()
        self._diff_queues()
        self._logger.print_process_status(complete=90)
        self._diff_tags()
        self._logger.print_process_status(complete=95)
        self._diff_wikis()
        self._logger.print_process_status(complete=100)

    def write_report(self, filename: str):
        if os.path.isfile(filename):
            os.remove(filename)
        with open(filename, "w", encoding="utf-8") as f:
            f.write('| object_type | object_id | message | base | comp |\n')
            self._write_report_diff_object(f, 'container', 'path', self.diff_containers)
            self._write_report_diff_object(f, 'source', 'name', self.diff_sources)
            self._write_report_diff_object(f, 'space', 'name', self.diff_spaces)
            self._write_report_diff_object(f, 'folder', 'path', self.diff_folders)
            self._write_report_diff_object(f, 'vds', 'path', self.diff_vds)
            self._write_report_diff_object(f, 'reflection', 'path', self.diff_reflections)
            self._write_report_diff_object(f, 'rule', 'name', self.diff_rules)
            self._write_report_diff_object(f, 'queue', 'name', self.diff_queues)
            self._write_report_diff_object(f, 'tag', 'path', self.diff_tags)
            self._write_report_diff_object(f, 'wiki', 'path', self.diff_wikis)

    def _write_report_diff_object(self, file, object_type: str, object_id: str, diff_object_list: list):
        for diff_object in diff_object_list:
            file.write('|' + object_type + '|' + diff_object['message'] + '|' +
                       str(diff_object['base']) + '|' + str(diff_object['comp']) + '|\n')

    def _diff_containers(self):
        self._diff_lists(self._base_def.containers, self._comp_def.containers, 'path', ['containerType'], self.diff_containers)
        return None

    def _diff_sources(self):
        fields = ['accelerationGracePeriodMs', 'accelerationNeverExpire', 'accelerationNeverRefresh',
                  'accelerationRefreshPeriodMs', 'accessControlList', 'allowCrossSourceSelection',
                  'checkTableAuthorizer', 'config', 'disableMetadataValidityCheck', 'entityType',
                  'metadataPolicy', 'owner', 'permissions', 'type']
        self._diff_lists(self._base_def.sources, self._comp_def.sources, 'name', fields, self.diff_sources)
        return None

    def _diff_spaces(self):
        fields = ['entityType', 'accessControlList', 'owner']
        self._diff_lists(self._base_def.spaces, self._comp_def.spaces, 'name', fields, self.diff_spaces)
        return None

    def _diff_folders(self):
        fields = ['entityType', 'accessControlList', 'owner']
        self._diff_lists(self._base_def.folders, self._comp_def.folders, 'path', fields, self.diff_folders)
        return None

    def _diff_vds(self):
        fields = ['entityType', 'accessControlList', 'owner', 'fields', 'sql', 'sqlContext', 'type']
        self._diff_lists(self._base_def.vds_list, self._comp_def.vds_list, 'path', fields, self.diff_vds)
        return None

    def _diff_reflections(self):
        fields = ['arrowCachingEnabled', 'canAlter', 'canView', 'enabled', 'entityType', 'name',
                  'partitionDistributionStrategy', 'type',
                  {'status': ['availability', 'combinedStatus', 'config', 'refresh']}]
        self._diff_lists(self._base_def.reflections, self._comp_def.reflections, 'path', fields, self.diff_reflections)
        return None

    def _diff_rules(self):
        fields = ['acceptName', 'action', 'conditions']
        self._diff_lists(self._base_def.rules, self._comp_def.rules, 'name', fields, self.diff_rules)
        return None

    def _diff_queues(self):
        fields = ['cpuTier', 'maxAllowedRunningJobs', 'maxStartTimeoutMs']
        self._diff_lists(self._base_def.queues, self._comp_def.queues, 'name', fields, self.diff_queues)
        return None

    def _diff_tags(self):
        fields = ['tags']
        self._diff_lists(self._base_def.tags, self._comp_def.tags, 'path', fields, self.diff_tags)
        return None

    def _diff_wikis(self):
        fields = ['text']
        self._diff_lists(self._base_def.wikis, self._comp_def.wikis, 'path', fields, self.diff_wikis)
        return None

    def _diff_lists(self, base_list: list, comp_list: list, uid, fields: [], report_list: list):
        for base_item in base_list:
            match_found = False
            for comp_item in comp_list:
                diff = self._diff_item(base_item, comp_item, uid, fields)
                if diff == DiffType.MATCH:
                    match_found = True
                    break
                elif diff == DiffType.DIFF_ATTRIBUTES:
                    self._report_diff(report_list, base_item, message='Different attributes.')
                    match_found = True
                    break
                elif diff == DiffType.MISSING_ATTRIBUTE:
                    self._report_diff(report_list, base_item, message='Missing attribute.')
                    match_found = True
                    break
            if not match_found:
                self._report_diff(report_list, base_item, message='Item is missing in Comp Environment')
        for comp_item in comp_list:
            match_found = False
            for base_item in base_list:
                diff = self._diff_item(base_item, comp_item, uid, fields)
                if diff != DiffType.DIFF_UID:  # Matching paths have been evaluated in prior loop
                    match_found = True
                    break
            if not match_found:
                self._report_diff(report_list, comp=comp_item, message='Extra item in Comp Environment')

    def _diff_item(self, base_item: dict, comp_item: dict, uid, fields: []) -> int:
        # Verify UID for recursive calls
        if uid is not None and base_item[uid] != comp_item[uid]:
            return DiffType.DIFF_UID
        for field in fields:
            if type(field) == dict:
                for key in field.keys():
                    diff = self._diff_item(base_item[key], comp_item[key], None, field[key])
                    if diff != DiffType.MATCH:
                        return diff
            elif field not in base_item and field not in comp_item:
                pass
            elif field not in base_item or field not in comp_item:
                return DiffType.MISSING_ATTRIBUTE
            elif field == 'accessControlList':
                diff_acl = self._diff_acl(base_item[field], comp_item[field])
                if diff_acl != DiffType.MATCH:
                    return diff_acl
            elif field == 'owner':
                dif_owner = self._diff_owner(base_item[field], comp_item[field])
                if dif_owner != DiffType.MATCH:
                    return dif_owner
            elif base_item[field] != comp_item[field]:
                return DiffType.DIFF_ATTRIBUTES
        return DiffType.MATCH

    def _diff_owner(self, base_owner, comp_owner) -> int:
        if base_owner['ownerType'] != comp_owner['ownerType']:
            return DiffType.DIFF_OWNER
        if base_owner['ownerType'] == 'USER':
            base_user_name = self._resolve_referenced_principal(base_owner['ownerId'], self._base_def.referenced_users)
            comp_user_name = self._resolve_referenced_principal(comp_owner['ownerId'], self._comp_def.referenced_users)
            if base_user_name == comp_user_name:
                return DiffType.MATCH
            else:
                return DiffType.DIFF_OWNER
        elif base_owner['ownerType'] == 'GROUP':
            base_group_name = self._resolve_referenced_principal(base_owner['ownerId'], self._base_def.referenced_groups)
            comp_group_name = self._resolve_referenced_principal(comp_owner['ownerId'], self._comp_def.referenced_groups)
            if base_group_name == comp_group_name:
                return DiffType.MATCH
            else:
                return DiffType.DIFF_OWNER
        elif base_owner['ownerType'] == 'ROLE':
            base_role_name = self._resolve_referenced_principal(base_owner['ownerId'], self._base_def.referenced_roles)
            comp_role_name = self._resolve_referenced_principal(comp_owner['ownerId'], self._comp_def.referenced_roles)
            if base_role_name == comp_role_name:
                return DiffType.MATCH
            else:
                return DiffType.DIFF_OWNER

    def _diff_acl(self, base_acl, comp_acl) -> int:
        diff_users = self._diff_acl_permissions('users', base_acl, comp_acl,
                                                self._base_def.referenced_users, self._comp_def.referenced_users)
        if diff_users != DiffType.MATCH:
            return diff_users
        diff_groups = self._diff_acl_permissions('groups', base_acl, comp_acl,
                                                 self._base_def.referenced_groups, self._comp_def.referenced_groups)
        if diff_groups != DiffType.MATCH:
            return diff_groups
        diff_roles = self._diff_acl_permissions('roles', base_acl, comp_acl,
                                                self._base_def.referenced_roles, self._comp_def.referenced_roles)
        if diff_roles != DiffType.MATCH:
            return diff_roles
        return DiffType.MATCH

    def _diff_acl_permissions(self, principal_type: str, base_acl, comp_acl, base_referenced_principals, comp_referenced_principals):
        if base_acl == {} and comp_acl == {}:
            return DiffType.MATCH
        if principal_type in base_acl and principal_type in comp_acl:
            for base_principal_acl in base_acl[principal_type]:
                base_principal_name = self._resolve_referenced_principal(base_principal_acl['id'], base_referenced_principals)
                match_found = False
                for comp_principal_acl in comp_acl[principal_type]:
                    comp_principal_name = self._resolve_referenced_principal(comp_principal_acl['id'], comp_referenced_principals)
                    if base_principal_name == comp_principal_name:
                        match_found = True
                        if base_principal_acl['permissions'] != comp_principal_acl['permissions']:
                            return DiffType.DIFF_PERMISSIONS
                        break
                if not match_found:
                    return DiffType.DIFF_PERMISSIONS
            return DiffType.MATCH
        elif principal_type not in base_acl and principal_type not in comp_acl:
            return DiffType.MATCH
        return DiffType.DIFF_PERMISSIONS

    def _resolve_referenced_principal(self, principal_id: str, referenced_principals: list) -> str:
        for principal in referenced_principals:
            if principal['id'] == principal_id:
                return principal['name']
        return None

    def _report_diff(self, report_list: list, base: dict = None, comp: dict = None, message: str = None):
        report_list.append({"base": base, "comp": comp, "message": message})
