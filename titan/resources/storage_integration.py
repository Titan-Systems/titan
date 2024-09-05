from dataclasses import dataclass

from ..enums import ParseableEnum, ResourceType
from ..props import BoolProp, EnumProp, Props, StringListProp, StringProp
from ..resource_name import ResourceName
from ..scope import AccountScope
from .resource import NamedResource, Resource, ResourceSpec
from .role import Role


class StorageProvider(ParseableEnum):
    S3 = "S3"
    AZURE = "AZURE"
    GCS = "GCS"


@dataclass(unsafe_hash=True)
class _S3StorageIntegration(ResourceSpec):
    name: ResourceName
    enabled: bool
    storage_aws_role_arn: str
    storage_allowed_locations: list[str]
    storage_blocked_locations: list[str] = None
    storage_provider: StorageProvider = StorageProvider.S3
    storage_aws_object_acl: str = None
    type: str = "EXTERNAL_STAGE"
    owner: Role = "ACCOUNTADMIN"
    comment: str = None

    def __post_init__(self):
        super().__post_init__()
        if self.type != "EXTERNAL_STAGE":
            raise ValueError("Type must be 'EXTERNAL_STAGE' for S3StorageIntegration")


class S3StorageIntegration(NamedResource, Resource):
    """
    Description:
        Manages the integration of Snowflake with S3 storage.

    Snowflake Docs:
        https://docs.snowflake.com/en/sql-reference/sql/create-storage-integration

    Fields:
        name (string, required): The name of the storage integration.
        enabled (bool, required): Whether the storage integration is enabled. Defaults to True.
        storage_aws_role_arn (string, required): The AWS IAM role ARN to access the S3 bucket.
        storage_allowed_locations (list, required): A list of allowed locations for storage in the format 's3://<bucket>/<path>/'.
        storage_blocked_locations (list): A list of blocked locations for storage in the format 's3://<bucket>/<path>/'. Defaults to an empty list.
        storage_aws_object_acl (string): The ACL policy for objects stored in S3. Defaults to 'bucket-owner-full-control'.
        type (string): The type of storage integration. Defaults to 'EXTERNAL_STAGE'.
        owner (string or Role): The owner role of the storage integration. Defaults to 'ACCOUNTADMIN'.
        comment (string): An optional comment about the storage integration.

    Python:

        ```python
        s3_storage_integration = S3StorageIntegration(
            name="some_s3_storage_integration",
            enabled=True,
            storage_aws_role_arn="arn:aws:iam::123456789012:role/MyS3AccessRole",
            storage_allowed_locations=["s3://mybucket/myfolder/"],
            storage_blocked_locations=["s3://mybucket/myblockedfolder/"],
            storage_aws_object_acl="bucket-owner-full-control",
            comment="This is a sample S3 storage integration."
        )
        ```

    Yaml:

        ```yaml
        s3_storage_integrations:
          - name: some_s3_storage_integration
            enabled: true
            storage_aws_role_arn: "arn:aws:iam::123456789012:role/MyS3AccessRole"
            storage_allowed_locations:
              - "s3://mybucket/myfolder/"
            storage_blocked_locations:
              - "s3://mybucket/myblockedfolder/"
            storage_aws_object_acl: "bucket-owner-full-control"
            comment: "This is a sample S3 storage integration."
        ```
    """

    resource_type = ResourceType.STORAGE_INTEGRATION
    props = Props(
        type=StringProp("type"),
        storage_provider=EnumProp("storage_provider", [StorageProvider.S3]),
        storage_aws_role_arn=StringProp("storage_aws_role_arn"),
        storage_aws_object_acl=StringProp("storage_aws_object_acl"),
        enabled=BoolProp("enabled"),
        storage_allowed_locations=StringListProp("storage_allowed_locations", parens=True),
        storage_blocked_locations=StringListProp("storage_blocked_locations", parens=True),
        comment=StringProp("comment"),
    )
    scope = AccountScope()
    spec = _S3StorageIntegration

    def __init__(
        self,
        name: str,
        enabled: bool,
        storage_aws_role_arn: str,
        storage_allowed_locations: list[str],
        storage_blocked_locations: list[str] = None,
        storage_aws_object_acl: str = None,
        owner: str = "ACCOUNTADMIN",
        comment: str = None,
        **kwargs,
    ):
        kwargs.pop("storage_provider", None)
        kwargs.pop("type", None)
        super().__init__(name, **kwargs)
        self._data: _S3StorageIntegration = _S3StorageIntegration(
            name=self._name,
            enabled=enabled,
            storage_aws_role_arn=storage_aws_role_arn,
            storage_allowed_locations=storage_allowed_locations,
            storage_blocked_locations=storage_blocked_locations,
            storage_aws_object_acl=storage_aws_object_acl,
            owner=owner,
            comment=comment,
        )


@dataclass(unsafe_hash=True)
class _GCSStorageIntegration(ResourceSpec):
    name: ResourceName
    enabled: bool
    storage_allowed_locations: list[str]
    storage_blocked_locations: list[str] = None
    storage_provider: StorageProvider = StorageProvider.GCS
    type: str = "EXTERNAL_STAGE"
    owner: Role = "ACCOUNTADMIN"
    comment: str = None

    def __post_init__(self):
        super().__post_init__()
        if self.type != "EXTERNAL_STAGE":
            raise ValueError("Type must be 'EXTERNAL_STAGE' for GCSStorageIntegration")


class GCSStorageIntegration(NamedResource, Resource):
    """
    Description:
        Manages the integration of Google Cloud Storage (GCS) as an external stage for storing data.

    Snowflake Docs:
        https://docs.snowflake.com/en/sql-reference/sql/create-storage-integration

    Fields:
        name (string, required): The name of the storage integration.
        enabled (bool, required): Specifies whether the storage integration is enabled.
        storage_allowed_locations (list): A list of allowed GCS locations for data storage.
        storage_blocked_locations (list): A list of blocked GCS locations for data storage.
        owner (string or Role): The owner role of the storage integration. Defaults to 'ACCOUNTADMIN'.
        comment (string): An optional comment about the storage integration.

    Python:

        ```python
        gcs_storage_integration = GCSStorageIntegration(
            name="some_gcs_storage_integration",
            enabled=True,
            storage_allowed_locations=['gcs://bucket/path/'],
            storage_blocked_locations=['gcs://bucket/blocked_path/']
        )
        ```

    Yaml:

        ```yaml
        gcs_storage_integrations:
          - name: some_gcs_storage_integration
            enabled: true
            storage_allowed_locations:
              - 'gcs://bucket/path/'
            storage_blocked_locations:
              - 'gcs://bucket/blocked_path/'
        ```
    """

    resource_type = ResourceType.STORAGE_INTEGRATION
    props = Props(
        type=StringProp("type"),
        storage_provider=EnumProp("storage_provider", [StorageProvider.GCS]),
        enabled=BoolProp("enabled"),
        storage_allowed_locations=StringListProp("storage_allowed_locations", parens=True),
        storage_blocked_locations=StringListProp("storage_blocked_locations", parens=True),
        comment=StringProp("comment"),
    )
    scope = AccountScope()
    spec = _GCSStorageIntegration

    def __init__(
        self,
        name: str,
        enabled: bool,
        storage_allowed_locations: list[str],
        storage_blocked_locations: list[str] = None,
        owner: str = "ACCOUNTADMIN",
        comment: str = None,
        **kwargs,
    ):
        kwargs.pop("storage_provider", None)
        kwargs.pop("type", None)
        super().__init__(name, **kwargs)
        self._data: _GCSStorageIntegration = _GCSStorageIntegration(
            name=self._name,
            enabled=enabled,
            storage_allowed_locations=storage_allowed_locations,
            storage_blocked_locations=storage_blocked_locations,
            owner=owner,
            comment=comment,
        )


@dataclass(unsafe_hash=True)
class _AzureStorageIntegration(ResourceSpec):
    name: ResourceName
    enabled: bool
    azure_tenant_id: str
    storage_allowed_locations: list[str]
    storage_blocked_locations: list[str] = None
    storage_provider: StorageProvider = StorageProvider.AZURE
    type: str = "EXTERNAL_STAGE"
    owner: Role = "ACCOUNTADMIN"
    comment: str = None

    def __post_init__(self):
        super().__post_init__()
        if self.type != "EXTERNAL_STAGE":
            raise ValueError("Type must be 'EXTERNAL_STAGE' for _AzureStorageIntegration")


class AzureStorageIntegration(NamedResource, Resource):
    """
    Description:
        Represents an Azure storage integration in Snowflake, which allows Snowflake to access external cloud storage using Azure credentials.

    Snowflake Docs:
        https://docs.snowflake.com/en/sql-reference/sql/create-storage-integration

    Fields:
        name (string, required): The name of the storage integration.
        enabled (bool, required): Specifies whether the storage integration is enabled.
        azure_tenant_id (string, required): The Azure tenant ID associated with the storage integration.
        storage_allowed_locations (list): The cloud storage locations that are allowed.
        storage_blocked_locations (list): The cloud storage locations that are blocked.
        owner (string or Role): The owner role of the storage integration. Defaults to "ACCOUNTADMIN".
        comment (string): A comment about the storage integration.

    Python:

        ```python
        azure_storage_integration = AzureStorageIntegration(
            name="some_azure_storage_integration",
            enabled=True,
            azure_tenant_id="some_tenant_id",
            storage_allowed_locations=["azure://somebucket/somepath/"],
            storage_blocked_locations=["azure://someotherbucket/somepath/"],
            comment="This is an Azure storage integration."
        )
        ```

    Yaml:

        ```yaml
        azure_storage_integrations:
          - name: some_azure_storage_integration
            enabled: true
            azure_tenant_id: some_tenant_id
            storage_allowed_locations:
              - azure://somebucket/somepath/
            storage_blocked_locations:
              - azure://someotherbucket/somepath/
            comment: This is an Azure storage integration.
        ```
    """

    resource_type = ResourceType.STORAGE_INTEGRATION
    props = Props(
        type=StringProp("type"),
        storage_provider=EnumProp("storage_provider", [StorageProvider.AZURE]),
        azure_tenant_id=StringProp("azure_tenant_id"),
        enabled=BoolProp("enabled"),
        storage_allowed_locations=StringListProp("storage_allowed_locations", parens=True),
        storage_blocked_locations=StringListProp("storage_blocked_locations", parens=True),
        comment=StringProp("comment"),
    )
    scope = AccountScope()
    spec = _AzureStorageIntegration

    def __init__(
        self,
        name: str,
        enabled: bool,
        azure_tenant_id: str,
        storage_allowed_locations: list[str],
        storage_blocked_locations: list[str] = None,
        owner: str = "ACCOUNTADMIN",
        comment: str = None,
        **kwargs,
    ):
        kwargs.pop("storage_provider", None)
        kwargs.pop("type", None)
        super().__init__(name, **kwargs)
        self._data: _AzureStorageIntegration = _AzureStorageIntegration(
            name=self._name,
            enabled=enabled,
            azure_tenant_id=azure_tenant_id,
            storage_allowed_locations=storage_allowed_locations,
            storage_blocked_locations=storage_blocked_locations,
            owner=owner,
            comment=comment,
        )


StorageIntegrationMap = {
    StorageProvider.S3: S3StorageIntegration,
    StorageProvider.GCS: GCSStorageIntegration,
    StorageProvider.AZURE: AzureStorageIntegration,
}


def _resolver(data: dict):
    return StorageIntegrationMap[StorageProvider(data["storage_provider"])]


Resource.__resolvers__[ResourceType.STORAGE_INTEGRATION] = _resolver
