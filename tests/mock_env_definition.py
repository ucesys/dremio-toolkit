from typing import List, Dict, Any

from env_definition import EnvDefinition


class MockEnvDefinition(EnvDefinition):
    def __init__(self):
        self.containers = self._containers()
        self.homes = self._homes()
        self.sources = self._sources()
        self.spaces = self._spaces()
        self.folders = self._folders()
        self.vds_list = self._vds()
        self.reflections = self._reflections()
        self.queues = self._queues()
        self.rules = self._rules()
        self.tags = self._tags()
        self.wikis = self._wikis()
        self.referenced_users = self._referenced_users()
        self.referenced_roles = self._referenced_roles()
        self.votes = []
        self.files = []
        self.dremio_get_config = []
        self.referenced_groups = []

    def _containers(self) -> List[Dict[str, Any]]:
        return [
            {
              "containerType": "HOME",
              "createdAt": "2023-02-03T21:08:05.131Z",
              "id": "4352b221-abcd-4ba9-a92d-b0dc1c2f36e4",
              "path": [
                  "@user"
              ],
              "tag": "S10NS8HStag=",
              "type": "CONTAINER"
            }
        ]

    def _homes(self) -> List[Dict[str, Any]]:
        return [
            {
                "children": [],
                "entityType": "home",
                "id": "4352b221-0aac-4ba9-a92d-b0dc1c2f36e4",
                "name": "@user",
                "tag": "S10NS8HSm9g="
            }
        ]

    def _sources(self) -> Dict[str, Any]:
        return {
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
            }

    def _spaces(self) -> List[Dict[str, Any]]:
        return [
            {
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
            }
        ]

    def _folders(self) -> List[Dict[str, Any]]:
        return [
            {
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
            }
        ]

    def _vds(self) -> List[Dict[str, Any]]:
        return [
            {
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
            }
        ]

    def _reflections(self) -> List[Dict[str, Any]]:
        return [
            {
                "arrowCachingEnabled": False,
                "canAlter": True,
                "canView": True,
                "createdAt": "2023-02-04T11:28:56.438Z",
                "currentSizeBytes": 0,
                "datasetId": "5bc7701d-e28b-4f47-900e-46b7c63cfe35",
                "enabled": False,
                "entityType": "reflection",
                "id": "bc39dfbb-ff24-4bde-89f1-0d9efe45d9b9",
                "name": "Aggregation Reflection",
                "partitionDistributionStrategy": "CONSOLIDATED",
                "path": [
                    "TestSpace",
                    "TaxiNY"
                ],
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

    def _queues(self) -> List[Dict[str, Any]]:
        return [
            {
                "cpuTier": "CRITICAL",
                "id": "4bfc8f5e-b255-4e73-9018-96c1e7f5b81f",
                "maxAllowedRunningJobs": 100,
                "maxStartTimeoutMs": 300000,
                "name": "UI Previews",
                "tag": "dMNrA1KnVF0="
            }
        ]

    def _rules(self) -> List[Dict[str, Any]]:
        return [
            {
                "acceptId": "4bfc8f5e-b255-4e73-9018-96c1e7f5b81f",
                "acceptName": "UI Previews",
                "action": "PLACE",
                "conditions": "query_type() = 'UI Preview'",
                "id": "0fccfd76-0e10-4a1f-ae03-1c847dc40a6b",
                "name": "UI Previews"
            }
        ]

    def _tags(self) -> List[Dict[str, Any]]:
        return [
            {
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
        ]

    def _wikis(self) -> List[Dict[str, Any]]:
        return [
            {
                "entity_id": "4352b221-0aac-4ba9-a92d-b0dc1c2f36e4",
                "path": [
                    "@mikhail"
                ],
                "text": "#  Wikis & Tags\n\n![Gnarly Catalog](https://d33wubrfki0l68.cloudfront.net/c1a54376c45a9276c080f3d10ed25ce61c17bcd2/2b946/img/home/open-source-for-everyone.svg)\n\nYou are reading the wiki for your home space! You can create and edit this information for any source, space, or folder.\n\nThis sidebar always shows the wiki for the current source, space or folder you are browsing.\n\nWhen previewing datasets, click on the `Catalog` tab to create a wiki or add tags to that dataset.\n\n**Tip:** You can hide the wiki by clicking on the sidebar icon on upper right hand side.",
                "version": 0
            }
        ]

    def _referenced_users(self) -> List[Dict[str, Any]]:
        return [
            {
                "@type": "EnterpriseUser",
                "active": True,
                "email": "mikhail@ucesys.com",
                "firstName": "M",
                "id": "cd6b0335-5113-485e-bf38-e75090857592",
                "lastName": "S",
                "name": "mikhail",
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
        ]


    def _referenced_roles(self) -> List[Dict[str, Any]]:
        return [
            {
                "description": "Predefined System Role",
                "id": "f71cfba5-e144-4090-883e-df878aca225e",
                "memberCount": 0,
                "name": "PUBLIC",
                "roles": [],
                "type": "SYSTEM"
            }
        ]
