from dataclasses import dataclass, field

from .resource import Resource, ResourceSpec, ResourceNameTrait
from .role import Role
from ..enums import ParseableEnum, ResourceType
from ..scope import SchemaScope
from ..resource_name import ResourceName
from ..props import (
    BoolProp,
    EnumProp,
    IdentifierProp,
    Props,
    PropSet,
    StringProp,
    TagsProp,
)


class StageType(ParseableEnum):
    INTERNAL = "INTERNAL"
    EXTERNAL = "EXTERNAL"


class EncryptionType(ParseableEnum):
    SNOWFLAKE_FULL = "SNOWFLAKE_FULL"
    SNOWFLAKE_SSE = "SNOWFLAKE_SSE"
    AWS_CSE = "AWS_CSE"
    AWS_SSE_S3 = "AWS_SSE_S3"
    AWS_SSE_KMS = "AWS_SSE_KMS"
    GCS_SSE_KMS = "GCS_SSE_KMS"
    AZURE_CSE = "AZURE_CSE"
    NONE = "NONE"


# """
# copyOptions ::=
#      ON_ERROR = { CONTINUE | SKIP_FILE | SKIP_FILE_<num> | 'SKIP_FILE_<num>%' | ABORT_STATEMENT }
#      SIZE_LIMIT = <num>
#      PURGE = TRUE | FALSE
#      RETURN_FAILED_ONLY = TRUE | FALSE
#      MATCH_BY_COLUMN_NAME = CASE_SENSITIVE | CASE_INSENSITIVE | NONE
#      ENFORCE_LENGTH = TRUE | FALSE
#      TRUNCATECOLUMNS = TRUE | FALSE
#      FORCE = TRUE | FALSE
# """
# copy_options = Props(
#     on_error=StringProp("on_error", alt_tokens=["CONTINUE", "SKIP_FILE", "ABORT_STATEMENT"]),
#     size_limit=IntProp("size_limit"),
#     purge=BoolProp("purge"),
#     return_failed_only=BoolProp("return_failed_only"),
#     match_by_column_name=StringProp("match_by_column_name", alt_tokens=["CASE_SENSITIVE", "CASE_INSENSITIVE", "NONE"]),
#     enforce_length=BoolProp("enforce_length"),
#     truncatecolumns=BoolProp("truncatecolumns"),
#     force=BoolProp("force"),
# )


@dataclass(unsafe_hash=True)
class _InternalStage(ResourceSpec):
    name: ResourceName
    owner: Role = "SYSADMIN"
    type: StageType = StageType.INTERNAL
    encryption: dict[str, EncryptionType] = field(default_factory=None, metadata={"fetchable": False})
    directory: dict[str, bool] = None
    tags: dict[str, str] = None
    comment: str = None

    def __post_init__(self):
        super().__post_init__()
        if self.type != StageType.INTERNAL:
            raise ValueError("Type must be INTERNAL for _InternalStage")
        if self.encryption:
            if "type" not in self.encryption:
                raise ValueError("When specifying encryption, 'type' is required")
            if self.encryption["type"] not in [EncryptionType.SNOWFLAKE_FULL, EncryptionType.SNOWFLAKE_SSE]:
                raise ValueError("Encryption 'type' must be SNOWFLAKE_FULL or SNOWFLAKE_SSE for InternalStage")


class InternalStage(ResourceNameTrait, Resource):
    """
    -- Internal stage
    CREATE [ OR REPLACE ] [ { TEMP | TEMPORARY } ] STAGE [ IF NOT EXISTS ] <internal_stage_name>
        internalStageParams
        directoryTableParams
        [ COMMENT = '<string_literal>' ]
        [ [ WITH ] TAG ( <tag_name> = '<tag_value>' [ , <tag_name> = '<tag_value>' , ... ] ) ]

    internalStageParams ::=
      [ ENCRYPTION = (TYPE = 'SNOWFLAKE_FULL' | TYPE = 'SNOWFLAKE_SSE') ]

    directoryTableParams (for internal stages) ::=
      [ DIRECTORY = ( ENABLE = { TRUE | FALSE }
                      [ REFRESH_ON_CREATE =  { TRUE | FALSE } ] ) ]
    """

    resource_type = ResourceType.STAGE
    props = Props(
        encryption=PropSet(
            "encryption",
            Props(type=EnumProp("type", [EncryptionType.SNOWFLAKE_FULL, EncryptionType.SNOWFLAKE_SSE], quoted=True)),
        ),
        directory=PropSet(
            "directory", Props(enable=BoolProp("ENABLE"), refresh_on_create=BoolProp("REFRESH_ON_CREATE"))
        ),
        comment=StringProp("comment"),
        tags=TagsProp(),
    )
    scope = SchemaScope()
    spec = _InternalStage

    def __init__(
        self,
        name: str,
        owner: str = "SYSADMIN",
        encryption: dict[str, EncryptionType] = None,
        directory: dict[str, bool] = None,
        tags: dict[str, str] = None,
        comment: str = None,
        **kwargs,
    ):
        kwargs.pop("type", None)
        super().__init__(name, **kwargs)
        self._data: _InternalStage = _InternalStage(
            name=self._name,
            owner=owner,
            encryption=encryption,
            directory=directory,
            tags=tags,
            comment=comment,
        )


@dataclass(unsafe_hash=True)
class _ExternalStage(ResourceSpec):
    name: ResourceName
    url: str
    owner: Role = "SYSADMIN"
    type: StageType = StageType.EXTERNAL
    storage_integration: str = None
    credentials: dict[str, str] = field(default_factory=None, metadata={"fetchable": False})
    encryption: dict[str, str] = field(default_factory=None, metadata={"fetchable": False})
    directory: dict[str, bool] = None
    tags: dict[str, str] = None
    comment: str = None

    def __post_init__(self):
        super().__post_init__()
        if self.type != StageType.EXTERNAL:
            raise ValueError("Type must be EXTERNAL for _ExternalStage")
        valid_encryption_types = ["AWS_CSE", "AWS_SSE_S3", "AWS_SSE_KMS", "GCS_SSE_KMS", "AZURE_CSE", "NONE"]
        if self.encryption and self.encryption["type"] not in valid_encryption_types:
            raise ValueError(
                f"Invalid encryption type: {self.encryption.get('type')}. Must be one of {valid_encryption_types}."
            )


class ExternalStage(ResourceNameTrait, Resource):
    """
    -- External stage
    CREATE [ OR REPLACE ] [ { TEMP | TEMPORARY } ] STAGE [ IF NOT EXISTS ] <external_stage_name>
        externalStageParams
        directoryTableParams
        [ COMMENT = '<string_literal>' ]
        [ [ WITH ] TAG ( <tag_name> = '<tag_value>' [ , <tag_name> = '<tag_value>' , ... ] ) ]

    externalStageParams (for Amazon S3) ::=
        URL = { 's3://<bucket>[/<path>/]' | 's3gov://<bucket>[/<path>/]' }

        [ { STORAGE_INTEGRATION = <integration_name> } | { CREDENTIALS = ( {  { AWS_KEY_ID = '<string>' AWS_SECRET_KEY = '<string>' [ AWS_TOKEN = '<string>' ] } | AWS_ROLE = '<string>'  } ) } ]
        [ ENCRYPTION = ( [ TYPE = 'AWS_CSE' ] [ MASTER_KEY = '<string>' ] |
                        [ TYPE = 'AWS_SSE_S3' ] |
                        [ TYPE = 'AWS_SSE_KMS' [ KMS_KEY_ID = '<string>' ] ] |
                        [ TYPE = 'NONE' ] ) ]

    externalStageParams (for Google Cloud Storage) ::=
        URL = 'gcs://<bucket>[/<path>/]'
        [ STORAGE_INTEGRATION = <integration_name> ]
        [ ENCRYPTION = ( [ TYPE = 'GCS_SSE_KMS' ] [ KMS_KEY_ID = '<string>' ] | [ TYPE = 'NONE' ] ) ]

    externalStageParams (for Microsoft Azure) ::=
        URL = 'azure://<account>.blob.core.windows.net/<container>[/<path>/]'
        [ { STORAGE_INTEGRATION = <integration_name> } | { CREDENTIALS = ( [ AZURE_SAS_TOKEN = '<string>' ] ) } ]
        [ ENCRYPTION = ( [ TYPE = 'AZURE_CSE' ] [ MASTER_KEY = '<string>' ] | [ TYPE = 'NONE' ] ) ]

    externalStageParams (for Amazon S3-compatible Storage) ::=
        URL = 's3compat://{bucket}[/{path}/]'
        ENDPOINT = '<s3_api_compatible_endpoint>'
        [ { CREDENTIALS = ( AWS_KEY_ID = '<string>' AWS_SECRET_KEY = '<string>' ) } ]

    directoryTableParams (for Amazon S3) ::=
        [ DIRECTORY = ( ENABLE = { TRUE | FALSE }
                        [ REFRESH_ON_CREATE =  { TRUE | FALSE } ]
                        [ AUTO_REFRESH = { TRUE | FALSE } ] ) ]

    directoryTableParams (for Google Cloud Storage) ::=
        [ DIRECTORY = ( ENABLE = { TRUE | FALSE }
                        [ AUTO_REFRESH = { TRUE | FALSE } ]
                        [ REFRESH_ON_CREATE =  { TRUE | FALSE } ]
                        [ NOTIFICATION_INTEGRATION = '<notification_integration_name>' ] ) ]

    directoryTableParams (for Microsoft Azure) ::=
        [ DIRECTORY = ( ENABLE = { TRUE | FALSE }
                        [ REFRESH_ON_CREATE =  { TRUE | FALSE } ]
                        [ AUTO_REFRESH = { TRUE | FALSE } ]
                        [ NOTIFICATION_INTEGRATION = '<notification_integration_name>' ] ) ]
    """

    resource_type = ResourceType.STAGE
    props = Props(
        url=StringProp("url"),
        storage_integration=IdentifierProp("storage_integration"),
        encryption=PropSet(
            "encryption",
            Props(
                type=StringProp("type"),
                master_key=StringProp("master_key"),
                kms_key_id=StringProp("kms_key_id"),
            ),
        ),
        credentials=PropSet(
            "credentials",
            Props(
                aws_key_id=StringProp("aws_key_id"),
                aws_secret_key=StringProp("aws_secret_key"),
                aws_token=StringProp("aws_token"),
                aws_role=StringProp("aws_role"),
            ),
        ),
        directory=PropSet(
            "directory",
            Props(
                enable=BoolProp("ENABLE"),
                refresh_on_create=BoolProp("REFRESH_ON_CREATE"),
            ),
        ),
        tags=TagsProp(),
        comment=StringProp("comment"),
    )
    scope = SchemaScope()
    spec = _ExternalStage

    def __init__(
        self,
        name: str,
        url: str,
        owner: str = "SYSADMIN",
        type: StageType = StageType.EXTERNAL,
        storage_integration: str = None,
        credentials: dict[str, str] = None,
        encryption: dict[str, EncryptionType] = None,
        directory: dict[str, bool] = None,
        tags: dict[str, str] = None,
        comment: str = None,
        **kwargs,
    ):
        kwargs.pop("type", None)
        super().__init__(name, **kwargs)
        if directory is None:
            directory = {"enable": False}
        self._data: _ExternalStage = _ExternalStage(
            name=self._name,
            url=url,
            owner=owner,
            type=type,
            storage_integration=storage_integration,
            credentials=credentials,
            encryption=encryption,
            directory=directory,
            tags=tags,
            comment=comment,
        )


StageTypeMap = {
    StageType.INTERNAL: InternalStage,
    StageType.EXTERNAL: ExternalStage,
}


def _resolver(data: dict):
    return StageTypeMap[StageType(data["type"])]


Resource.__resolvers__[ResourceType.STAGE] = _resolver
