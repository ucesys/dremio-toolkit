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

from typing import Optional, Dict, Any

from dremio_toolkit.env_api import EnvApi


class MockEnvApi(EnvApi):
    def __init__(self):
        pass

    def list_rules(self) -> Dict[str, Any]:
        return {
            "rules": [
                {
                    "acceptId": "4bfc8f5e-b255-4e73-9018-96c1e7f5b81f",
                    "acceptName": "UI Previews",
                    "action": "PLACE",
                    "conditions": "query_type() = 'UI Preview'",
                    "id": "0fccfd76-0e10-4a1f-ae03-1c847dc40a6b",
                    "name": "UI Previews"
                }
            ]
        }

    def list_queues(self) -> Dict[str, Any]:
        return {
            "data": [
                {
                    "cpuTier": "CRITICAL",
                    "id": "4bfc8f5e-b255-4e73-9018-96c1e7f5b81f",
                    "maxAllowedRunningJobs": 100,
                    "maxStartTimeoutMs": 300000,
                    "name": "UI Previews",
                    "tag": "dMNrA1KnVF0="
                }
            ]
        }

    def list_votes(self) -> Dict[str, Any]:
        return {"data": []}

    def get_env_endpoint(self) -> str:
        return "http://localhost:9047/"

    def get_catalog_tags(self, catalog_id) -> Dict[str, Any]:
        return {
            "entity_id": "5bc7701d-e28b-4f47-900e-46b7c63cfe35",
            "path": [
                "TestSpace",
                "TaxiNY"
            ],
            "tags": [
                "Tag1",
                "Tag2"
            ],
            "version": "1mnAO+r2rrI="
        }

    def get_catalog_wiki(self, catalog_id) -> Dict[str, Any]:
        return {
            "entity_id": "4352b221-0aac-4ba9-a92d-b0dc1c2f36e4",
            "path": [
                "@user"
            ],
            "text": "#  Wikis & Tags\n\n![Gnarly Catalog](https://d33wubrfki0l68.cloudfront.net/c1a54376c45a9276c080f3d10ed25ce61c17bcd2/2b946/img/home/open-source-for-everyone.svg)\n\nYou are reading the wiki for your home space! You can create and edit this information for any source, space, or folder.\n\nThis sidebar always shows the wiki for the current source, space or folder you are browsing.\n\nWhen previewing datasets, click on the `Catalog` tab to create a wiki or add tags to that dataset.\n\n**Tip:** You can hide the wiki by clicking on the sidebar icon on upper right hand side.",
            "version": 0
        }

    def get_group(self, group_id) -> Optional[str]:
        return None

    def get_role(self, role_id) -> Dict[str, Any]:
        return {
            "description": "Predefined System Role",
            "id": "f71cfba5-e144-4090-883e-df878aca225e",
            "memberCount": 0,
            "name": "PUBLIC",
            "roles": [],
            "type": "SYSTEM"
        }

    def get_user(self, user_id) -> Dict[str, Any]:
        return {
            "@type": "EnterpriseUser",
            "active": True,
            "email": "user123@mydomain.com",
            "firstName": "M",
            "id": "cd6b0335-5113-485e-bf38-e75090857592",
            "lastName": "S",
            "name": "user123",
            "roles": [
                {
                    "id": "f71cfba5-e144-4090-883e-df878aca225e",
                    "name": "PUBLIC",
                    "type": "SYSTEM"
                }
            ],
            "source": "local",
            "tag": "Ee65lPjZOpU="
        }

    def get_catalog_graph(self, catalog_id):
        return {
            "parents": [
                {"path": "/path/to/dataset"},
                {"path": "/other/path/to/dataset"}
            ]
        }

    def list_catalogs(self):
        return {
            "data": [
                {
                  "containerType": "HOME",
                  "createdAt": "2023-02-03T21:08:05.131Z",
                  "id": "4352b221-abcd-4ba9-a92d-b0dc1c2f36e4",
                  "path": [
                      "@user"
                  ],
                  "tag": "S10NS8HStag=",
                  "type": "CONTAINER"
                },
                {
                    "containerType": "SPACE",
                    "createdAt": "2023-02-03T21:36:29.932Z",
                    "id": "86b7f8ff-cdaa-455c-9d12-51ec0dbdcf4f",
                    "path": [
                        "TestSpace"
                    ],
                    "tag": "jhEcARN4ZW0=",
                    "type": "CONTAINER"
                },
                {
                    "containerType": "SOURCE",
                    "createdAt": "2023-02-03T21:37:24.996Z",
                    "id": "01b1d888-1165-4fe7-b4cc-712688e94dad",
                    "path": [
                        "LocalData"
                    ],
                    "tag": "JDLrVceDJ68=",
                    "type": "CONTAINER"
                }
            ]
        }

    def get_catalog(self, catalog_id):
        catalogs = {
            "4352b221-abcd-4ba9-a92d-b0dc1c2f36e4": {
                "children": [],
                "entityType": "home",
                "id": "4352b221-0aac-4ba9-a92d-b0dc1c2f36e4",
                "name": "@user",
                "tag": "S10NS8HSm9g="
            },
            "86b7f8ff-cdaa-455c-9d12-51ec0dbdcf4f": {
                "accessControlList": {
                    "roles": [
                        {
                            "id": "f71cfba5-e144-4090-883e-df878aca225e",
                            "permissions": [
                                "SELECT"
                            ]
                        },
                        {
                            "id": "a1fc3e27-2bac-4ad0-b03a-a07ba0e851bb",
                            "permissions": [
                                "VIEW_REFLECTION",
                                "ALTER_REFLECTION",
                                "MODIFY",
                                "MANAGE_GRANTS",
                                "ALTER"
                            ]
                        }
                    ]
                },
                "children": [
                    {
                        "containerType": "FOLDER",
                        "id": "9e5a0edd-f280-4ef6-afb9-7d5998e9cbaa",
                        "path": [
                            "TestSpace",
                            "folder1"
                        ],
                        "tag": "kad/yd+PUUU=",
                        "type": "CONTAINER"
                    },
                    {
                        "createdAt": "2023-02-03T22:22:30.355Z",
                        "datasetType": "VIRTUAL",
                        "id": "9a57f624-b607-4834-bc3d-76e00dcb55dd",
                        "path": [
                            "TestSpace",
                            "TaxiNY"
                        ],
                        "tag": "g6BX4qw0UVo=",
                        "type": "DATASET"
                    }
                ],
                "createdAt": "2023-02-03T21:36:29.932Z",
                "entityType": "space",
                "id": "86b7f8ff-cdaa-455c-9d12-51ec0dbdcf4f",
                "name": "TestSpace",
                "owner": {
                    "ownerId": "cd6b0335-5113-485e-bf38-e75090857592",
                    "ownerType": "USER"
                },
                "tag": "jhEcARN4ZW0="
            },
            "01b1d888-1165-4fe7-b4cc-712688e94dad": {
                "accelerationGracePeriodMs": 10800000,
                "accelerationNeverExpire": False,
                "accelerationNeverRefresh": False,
                "accelerationRefreshPeriodMs": 3600000,
                "accessControlList": {},
                "allowCrossSourceSelection": False,
                "checkTableAuthorizer": False,
                "children": [
                    {
                        "datasetType": "PROMOTED",
                        "id": "3571d97f-8549-4634-a6f4-ad04ad8877ca",
                        "path": [
                            "LocalData",
                            "nyc_taxi_trips"
                        ],
                        "type": "DATASET"
                    }
                ],
                "config": {
                    "defaultCtasFormat": "ICEBERG",
                    "isPartitionInferenceEnabled": True,
                    "path": "/opt/dremio23/data/local_data"
                },
                "createdAt": "2023-02-03T21:37:24.996Z",
                "disableMetadataValidityCheck": False,
                "entityType": "source",
                "id": "01b1d888-1165-4fe7-b4cc-712688e94dad",
                "metadataPolicy": {
                    "authTTLMs": 86400000,
                    "autoPromoteDatasets": True,
                    "datasetExpireAfterMs": 604195200000,
                    "datasetRefreshAfterMs": 604195200000,
                    "datasetUpdateMode": "PREFETCH_QUERIED",
                    "deleteUnavailableDatasets": True,
                    "namesRefreshMs": 3600000
                },
                "name": "LocalData",
                "owner": {
                    "ownerId": "cd6b0335-5113-485e-bf38-e75090857592",
                    "ownerType": "USER"
                },
                "permissions": [],
                "tag": "JDLrVceDJ68=",
                "type": "NAS"
            },
            "9e5a0edd-f280-4ef6-afb9-7d5998e9cbaa": {
                "accessControlList": {},
                "children": [
                    {
                        "containerType": "FOLDER",
                        "id": "0ce636ab-67a0-4084-9485-69b282f0ff5e",
                        "path": [
                            "TestSpace",
                            "folder1",
                            "folder2"
                        ],
                        "tag": "tuo9YuK4+fc=",
                        "type": "CONTAINER"
                    }
                ],
                "entityType": "folder",
                "id": "9e5a0edd-f280-4ef6-afb9-7d5998e9cbaa",
                "owner": {
                    "ownerId": "cd6b0335-5113-485e-bf38-e75090857592",
                    "ownerType": "USER"
                },
                "path": [
                    "TestSpace",
                    "folder1"
                ],
                "tag": "kad/yd+PUUU="
            },
            "9a57f624-b607-4834-bc3d-76e00dcb55dd": {
                "accessControlList": {},
                "createdAt": "2023-02-03T22:23:06.511Z",
                "entityType": "dataset",
                "fields": [
                    {
                        "name": "pickup_datetime",
                        "type": {
                            "name": "TIMESTAMP"
                        }
                    },
                    {
                        "name": "passenger_count",
                        "type": {
                            "name": "BIGINT"
                        }
                    },
                    {
                        "name": "trip_distance_mi",
                        "type": {
                            "name": "DOUBLE"
                        }
                    },
                    {
                        "name": "fare_amount",
                        "type": {
                            "name": "DOUBLE"
                        }
                    },
                    {
                        "name": "tip_amount",
                        "type": {
                            "name": "DOUBLE"
                        }
                    },
                    {
                        "name": "total_amount",
                        "type": {
                            "name": "DOUBLE"
                        }
                    }
                ],
                "id": "9a57f624-b607-4834-bc3d-76e00dcb55dd",
                "owner": {
                    "ownerId": "cd6b0335-5113-485e-bf38-e75090857592",
                    "ownerType": "USER"
                },
                "path": [
                    "TestSpace",
                    "MyFolder",
                    "TaxiTrips"
                ],
                "sql": "SELECT * FROM nyc_taxi_trips",
                "sqlContext": [
                    "LocalData"
                ],
                "tag": "zx7nGPwA6/M=",
                "type": "VIRTUAL_DATASET"
            },
            "5bc7701d-e28b-4f47-900e-46b7c63cfe35": {
                "path": [
                    "TestSpace",
                    "TaxiNY"
                ]
            }
        }
        return catalogs.get(catalog_id)

    def list_reflections(self):
        return {
            "data": [
                {
                    "arrowCachingEnabled": False,
                    "canAlter": True,
                    "canView": True,
                    "createdAt": "2023-02-04T11:28:56.438Z",
                    "currentSizeBytes": 0,
                    "datasetId": "5bc7701d-e28b-4f47-900e-46b7c63cfe35",
                    "enabled": True,
                    "entityType": "reflection",
                    "id": "bc39dfbb-ff24-4bde-89f1-0d9efe45d9b9",
                    "name": "Aggregation Reflection",
                    "partitionDistributionStrategy": "CONSOLIDATED",
                    "status": {
                        "availability": "NONE",
                        "combinedStatus": "DISABLED",
                        "config": "OK",
                        "expiresAt": "1970-01-01T00:00:00.000Z",
                        "failureCount": 0,
                        "lastDataFetch": "1970-01-01T00:00:00.000Z",
                        "refresh": "SCHEDULED"
                    },
                    "tag": "i/DCEiV9F88=",
                    "totalSizeBytes": 0,
                    "type": "AGGREGATION",
                    "updatedAt": "2023-02-04T11:28:56.438Z"
                }
            ]
        }
