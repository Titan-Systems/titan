from dataclasses import dataclass, field

from ..enums import ParseableEnum, ResourceType, EncryptionType
from ..props import (
    BoolProp,
    EnumProp,
    IdentifierProp,
    Props,
    PropSet,
    StringProp,
    TagsProp,
)
from ..resource_name import ResourceName
from ..role_ref import RoleRef
from ..scope import SchemaScope
from .resource import NamedResource, Resource, ResourceSpec
from .tag import TaggableResource


class StageType(ParseableEnum):
    INTERNAL = "INTERNAL"
    EXTERNAL = "EXTERNAL"


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
    owner: RoleRef = "SYSADMIN"
    type: StageType = StageType.INTERNAL
    encryption: dict[str, EncryptionType] = field(default=None, metadata={"fetchable": False})
    directory: dict[str, bool] = None
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
        if self.directory is None:
            self.directory = {"enable": False}


class InternalStage(NamedResource, TaggableResource, Resource):
    """
    Description:
        Represents an internal stage in Snowflake, which is a named location used to store data files
        that will be loaded into or unloaded from Snowflake tables.

    Snowflake Docs:
        https://docs.snowflake.com/en/sql-reference/sql/create-stage.html

    Fields:
        name (string, required): The name of the internal stage.
        owner (string or Role): The owner role of the internal stage. Defaults to "SYSADMIN".
        encryption (dict): A dictionary specifying encryption settings.
        directory (dict): A dictionary specifying directory usage settings.
        tags (dict): A dictionary of tags associated with the internal stage.
        comment (string): A comment for the internal stage.

    Python:

        ```python
        internal_stage = InternalStage(
            name="some_internal_stage",
            owner="SYSADMIN",
            encryption={"type": "SNOWFLAKE_SSE"},
            directory={"enable": True},
            tags={"department": "finance"},
            comment="Data loading stage"
        )
        ```

    Yaml:

        ```yaml
        stages:
          - name: some_internal_stage
            type: internal
            owner: SYSADMIN
            encryption:
              type: SNOWFLAKE_SSE
            directory:
              enable: true
            tags:
              department: finance
            comment: Data loading stage
        ```
    """

    resource_type = ResourceType.STAGE
    props = Props(
        encryption=PropSet(
            "encryption",
            Props(type=EnumProp("type", [EncryptionType.SNOWFLAKE_FULL, EncryptionType.SNOWFLAKE_SSE], quoted=True)),
        ),
        directory=PropSet(
            "directory",
            Props(
                enable=BoolProp("ENABLE"),
                refresh_on_create=BoolProp("REFRESH_ON_CREATE"),
            ),
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
            comment=comment,
        )
        self.set_tags(tags)


@dataclass
class _ExternalStage(ResourceSpec):
    name: ResourceName
    url: str
    owner: RoleRef = "SYSADMIN"
    type: StageType = StageType.EXTERNAL
    storage_integration: str = None
    credentials: dict[str, str] = field(default=None, metadata={"fetchable": False})
    encryption: dict[str, str] = field(default=None, metadata={"fetchable": False})
    directory: dict[str, bool] = None
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
        if self.directory is None:
            self.directory = {"enable": False}


class ExternalStage(NamedResource, TaggableResource, Resource):
    """
    Description:
        Manages external stages in Snowflake, which are used to reference external storage locations.

    Snowflake Docs:
        https://docs.snowflake.com/en/sql-reference/sql/create-stage

    Fields:
        name (string, required): The name of the external stage.
        url (string, required): The URL pointing to the external storage location.
        owner (string or Role): The owner role of the external stage. Defaults to "SYSADMIN".
        storage_integration (string): The name of the storage integration used with this stage.
        credentials (dict): The credentials for accessing the external storage, if required.
        encryption (dict): The encryption settings used for data stored in the external location.
        directory (dict): Settings related to directory handling in the external storage.
        tags (dict): Tags associated with the external stage.
        comment (string): A comment about the external stage.

    Python:

        ```python
        external_stage = ExternalStage(
            name="some_external_stage",
            url="https://example.com/storage",
            owner="SYSADMIN",
            storage_integration="some_integration"
        )
        ```

    Yaml:

        ```yaml
        stages:
          - name: some_external_stage
            type: external
            url: https://example.com/storage
            owner: SYSADMIN
            storage_integration: some_integration
        ```
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
            comment=comment,
        )
        self.set_tags(tags)


StageTypeMap = {
    StageType.INTERNAL: InternalStage,
    StageType.EXTERNAL: ExternalStage,
}


def _resolver(data: dict):
    return StageTypeMap[StageType(data["type"])]


Resource.__resolvers__[ResourceType.STAGE] = _resolver
