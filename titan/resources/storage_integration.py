from typing import List, Union

from . import Resource
from .base import AccountScoped
from ..parse import _resolve_resource_class
from ..props import Props, StringProp, BoolProp, EnumProp, StringListProp
from ..enums import ParseableEnum


class StorageProvider(ParseableEnum):
    S3 = "S3"
    AZURE = "AZURE"
    GCS = "GCS"


class S3StorageIntegration(Resource, AccountScoped):
    """
    CREATE [ OR REPLACE ] STORAGE INTEGRATION [IF NOT EXISTS]
      <name>
      TYPE = EXTERNAL_STAGE
      cloudProviderParams
      ENABLED = { TRUE | FALSE }
      STORAGE_ALLOWED_LOCATIONS = ('<cloud>://<bucket>/<path>/' [ , '<cloud>://<bucket>/<path>/' ... ] )
      [ STORAGE_BLOCKED_LOCATIONS = ('<cloud>://<bucket>/<path>/' [ , '<cloud>://<bucket>/<path>/' ... ] ) ]
      [ COMMENT = '<string_literal>' ]

    cloudProviderParams (for Amazon S3) ::=
      STORAGE_PROVIDER = 'S3'
      STORAGE_AWS_ROLE_ARN = '<iam_role>'
      [ STORAGE_AWS_OBJECT_ACL = 'bucket-owner-full-control' ]
    """

    resource_type = "STORAGE INTEGRATION"
    props = Props(
        _start_token="type = external_stage",
        storage_provider=EnumProp("storage_provider", [StorageProvider.S3]),
        storage_aws_role_arn=StringProp("storage_aws_role_arn"),
        storage_aws_object_acl=StringProp("storage_aws_object_acl"),
        enabled=BoolProp("enabled"),
        storage_allowed_locations=StringListProp("storage_allowed_locations"),
        storage_blocked_locations=StringListProp("storage_blocked_locations"),
        comment=StringProp("comment"),
    )

    name: str
    owner: str = None
    storage_provider: StorageProvider
    storage_aws_role_arn: str
    storage_aws_object_acl: str = None
    enabled: bool
    storage_allowed_locations: List[str]
    storage_blocked_locations: List[str] = None
    comment: str = None


class GCSStorageIntegration(Resource, AccountScoped):
    """
    CREATE [ OR REPLACE ] STORAGE INTEGRATION [IF NOT EXISTS]
      <name>
      TYPE = EXTERNAL_STAGE
      cloudProviderParams
      ENABLED = { TRUE | FALSE }
      STORAGE_ALLOWED_LOCATIONS = ('<cloud>://<bucket>/<path>/' [ , '<cloud>://<bucket>/<path>/' ... ] )
      [ STORAGE_BLOCKED_LOCATIONS = ('<cloud>://<bucket>/<path>/' [ , '<cloud>://<bucket>/<path>/' ... ] ) ]
      [ COMMENT = '<string_literal>' ]

    cloudProviderParams (for Google Cloud Storage) ::=
      STORAGE_PROVIDER = 'GCS'

    """

    resource_type = "STORAGE INTEGRATION"
    props = Props(
        _start_token="type = external_stage",
        storage_provider=EnumProp("storage_provider", [StorageProvider.GCS]),
        enabled=BoolProp("enabled"),
        storage_allowed_locations=StringListProp("storage_allowed_locations"),
        storage_blocked_locations=StringListProp("storage_blocked_locations"),
        comment=StringProp("comment"),
    )

    name: str
    owner: str = None
    storage_provider: StorageProvider
    enabled: bool
    storage_allowed_locations: List[str]
    storage_blocked_locations: List[str] = None
    comment: str = None


class AzureStorageIntegration(Resource, AccountScoped):
    """
    CREATE [ OR REPLACE ] STORAGE INTEGRATION [IF NOT EXISTS]
      <name>
      TYPE = EXTERNAL_STAGE
      cloudProviderParams
      ENABLED = { TRUE | FALSE }
      STORAGE_ALLOWED_LOCATIONS = ('<cloud>://<bucket>/<path>/' [ , '<cloud>://<bucket>/<path>/' ... ] )
      [ STORAGE_BLOCKED_LOCATIONS = ('<cloud>://<bucket>/<path>/' [ , '<cloud>://<bucket>/<path>/' ... ] ) ]
      [ COMMENT = '<string_literal>' ]

    cloudProviderParams (for Microsoft Azure) ::=
      STORAGE_PROVIDER = 'AZURE'
      AZURE_TENANT_ID = '<tenant_id>'
    """

    resource_type = "STORAGE INTEGRATION"
    props = Props(
        _start_token="type = external_stage",
        storage_provider=EnumProp("storage_provider", [StorageProvider.AZURE]),
        azure_tenant_id=StringProp("azure_tenant_id"),
        enabled=BoolProp("enabled"),
        storage_allowed_locations=StringListProp("storage_allowed_locations"),
        storage_blocked_locations=StringListProp("storage_blocked_locations"),
        comment=StringProp("comment"),
    )

    name: str
    owner: str = None
    storage_provider: StorageProvider
    azure_tenant_id: str
    enabled: bool
    storage_allowed_locations: List[str]
    storage_blocked_locations: List[str] = None
    comment: str = None


StorageIntegrationMap = {
    StorageProvider.S3: S3StorageIntegration,
    StorageProvider.GCS: GCSStorageIntegration,
    StorageProvider.AZURE: AzureStorageIntegration,
}


class StorageIntegration(Resource):
    def __init__(self, *args, **kwargs):
        raise NotImplementedError

    def __new__(
        cls, storage_provider: Union[str, StorageProvider], **kwargs
    ) -> Union[S3StorageIntegration, GCSStorageIntegration, AzureStorageIntegration]:
        storage_provider_type = StorageProvider.parse(storage_provider)
        storage_integration_cls = StorageIntegrationMap[storage_provider_type]
        return storage_integration_cls(type=storage_provider_type, **kwargs)

    @classmethod
    def from_sql(cls, sql):
        resource_cls = Resource.classes[_resolve_resource_class(sql)]
        return resource_cls.from_sql(sql)
