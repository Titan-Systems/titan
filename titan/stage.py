# import re

# from typing import List, Optional, Tuple, Union

# from .parseable_enum import ParseableEnum
# from .props import (
#     StringProp,
#     TagsProp,
#     Identifier,
#     EnumProp,
#     PropSet,
#     IntProp,
#     BoolProp,
#     IdentifierProp,
#     # AnonFileFormatProp,
#     FileFormatProp,
# )
# from .resource import SchemaLevelResource
# from .file_format import FileFormat

from typing import Dict

from .props import (
    BoolProp,
    EnumProp,
    IdentifierProp,
    IntProp,
    ParseableEnum,
    Props,
    StringProp,
    TagsProp,
)


from .resource2 import Resource, Namespace


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


class Stage(Resource):
    """
    -- Internal stage
    CREATE [ OR REPLACE ] [ { TEMP | TEMPORARY } ] STAGE [ IF NOT EXISTS ] <internal_stage_name>
        internalStageParams
        directoryTableParams
      [ FILE_FORMAT = ( { FORMAT_NAME = '<file_format_name>' | TYPE = { CSV | JSON | AVRO | ORC | PARQUET | XML } [ formatTypeOptions ] } ) ]
      [ COPY_OPTIONS = ( copyOptions ) ]
      [ [ WITH ] TAG ( <tag_name> = '<tag_value>' [ , <tag_name> = '<tag_value>' , ... ] ) ]
      [ COMMENT = '<string_literal>' ]

    -- External stage
    CREATE [ OR REPLACE ] [ { TEMP | TEMPORARY } ] STAGE [ IF NOT EXISTS ] <external_stage_name>
        externalStageParams
        directoryTableParams
      [ FILE_FORMAT = ( { FORMAT_NAME = '<file_format_name>' | TYPE = { CSV | JSON | AVRO | ORC | PARQUET | XML } [ formatTypeOptions ] } ) ]
      [ COPY_OPTIONS = ( copyOptions ) ]
      [ [ WITH ] TAG ( <tag_name> = '<tag_value>' [ , <tag_name> = '<tag_value>' , ... ] ) ]
      [ COMMENT = '<string_literal>' ]

    internalStageParams ::=
      [ ENCRYPTION = (TYPE = 'SNOWFLAKE_FULL' | TYPE = 'SNOWFLAKE_SSE') ]

    externalStageParams (for Amazon S3) ::=
      URL = { 's3://<bucket>[/<path>/]' | 's3gov://<bucket>[/<path>/]' }

      [ { STORAGE_INTEGRATION = <integration_name> } | { CREDENTIALS = ( {  { AWS_KEY_ID = '<string>' AWS_SECRET_KEY = '<string>' [ AWS_TOKEN = '<string>' ] } | AWS_ROLE = '<string>'  } ) ) } ]
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

    copyOptions ::=
         ON_ERROR = { CONTINUE | SKIP_FILE | SKIP_FILE_<num> | 'SKIP_FILE_<num>%' | ABORT_STATEMENT }
         SIZE_LIMIT = <num>
         PURGE = TRUE | FALSE
         RETURN_FAILED_ONLY = TRUE | FALSE
         MATCH_BY_COLUMN_NAME = CASE_SENSITIVE | CASE_INSENSITIVE | NONE
         ENFORCE_LENGTH = TRUE | FALSE
         TRUNCATECOLUMNS = TRUE | FALSE
         FORCE = TRUE | FALSE

    """

    resource_type = "STAGE"
    namespace = Namespace.SCHEMA

    name: str
    owner: str = None

    # def __init__(self, stage_type: Optional[StageType] = None, **kwargs):
    #     # if type(self) == FileFormat:
    #     #     raise TypeError(f"only children of '{type(self).__name__}' may be instantiated")
    #     self.stage_type = stage_type or StageType.INTERNAL
    #     super().__init__(**kwargs)

    # @classmethod
    # def from_sql(cls, sql: str):
    #     match = re.search(cls.create_statement, sql)

    #     if match is None:
    #         raise Exception
    #     name = match.group(1)
    #     # url = StringProp("URL").search(sql[match.end() :])
    #     # stage_type = StageType.EXTERNAL if url else StageType.INTERNAL
    #     # try:
    #     props = InternalStage.parse_props(sql[match.end() :])
    #     return InternalStage(name=name, **props)
    #     # except Exception:
    #     #     print("&" * 120)
    #     #     props = ExternalStage.parse_props(sql[match.end() :])
    #     #     return ExternalStage(name=name, **props)

    #     if stage_type == StageType.INTERNAL:
    #         props = InternalStage.parse_props(sql[match.end() :])
    #         return InternalStage(name=name, **props)
    #     else:
    #         props = ExternalStage.parse_props(sql[match.end() :])
    #         return ExternalStage(name=name, **props)

    def props_sql(self):
        props = self.props.copy()
        del props["FILE_FORMAT"]
        format_sql = ""
        if self.file_format:
            format_sql = ""
        return self._props_sql(props) + format_sql

    # @property
    # def on_file_added(self):
    #     return self.hooks["on_file_added"]

    # @on_file_added.setter
    # def on_file_added(self, hook):
    #     # TODO: This needs to be refactored to be wrapped in a Sproc resource and for dependencies to be implicitly tracked
    #     self.hooks["on_file_added"] = on_file_added_factory("ZIPPED_TRIPS", hook)
    #     self.state["on_file_added:last_checked"] = State(key="last_checked", value="'1900-01-01'::DATETIME")
    #     # print("on_file_added")
    #     # print(inspect.getsource(self.hooks["on_file_added"]))

    # def create(self, session):
    #     super().create(session)

    #     for statefunc in self.state.values():
    #         statefunc.create(session)

    #     if self.hooks["on_file_added"]:
    #         session.sql("CREATE STAGE IF NOT EXISTS sprocs").collect()
    #         session.add_packages("snowflake-snowpark-python")
    #         session.sproc.register(
    #             self.hooks["on_file_added"],
    #             name=f"ZIPPED_TRIPS_hook_on_file_added",
    #             replace=True,
    #             is_permanent=True,
    #             stage_location="@sprocs",
    #             execute_as="caller",
    #         )


_copy_options = PropSet(
    "COPY_OPTIONS",
    {
        "ON_ERROR": StringProp("ON_ERROR"),
        "SIZE_LIMIT": IntProp("SIZE_LIMIT"),
        "PURGE": BoolProp("PURGE"),
        "RETURN_FAILED_ONLY": BoolProp("RETURN_FAILED_ONLY"),
        # "MATCH_BY_COLUMN_NAME": EnumProp("MATCH_BY_COLUMN_NAME", ["CASE_SENSITIVE", "CASE_INSENSITIVE", "NONE"]),
        "ENFORCE_LENGTH": BoolProp("ENFORCE_LENGTH"),
        "TRUNCATECOLUMNS": BoolProp("TRUNCATECOLUMNS"),
        "FORCE": BoolProp("FORCE"),
    },
)


class InternalStage(Stage):
    """
    directoryTableParams (for internal stages) ::=
      [ DIRECTORY = ( ENABLE = { TRUE | FALSE }
                      [ REFRESH_ON_CREATE =  { TRUE | FALSE } ] ) ]
    """

    props = {
        "ENCRYPTION": PropSet(
            "ENCRYPTION",
            {"TYPE": EnumProp("TYPE", [EncryptionType.SNOWFLAKE_FULL, EncryptionType.SNOWFLAKE_SSE])},
        ),
        "DIRECTORY": PropSet(
            "DIRECTORY", {"ENABLE": BoolProp("ENABLE"), "REFRESH_ON_CREATE": BoolProp("REFRESH_ON_CREATE")}
        ),
        "FILE_FORMAT": [
            IdentifierProp("FILE_FORMAT"),
            PropSet("FILE_FORMAT", {"FORMAT_NAME": IdentifierProp("FORMAT_NAME")}),
            # AnonFileFormatProp("FILE_FORMAT"),
        ],
        "COPY_OPTIONS": _copy_options,
        "TAGS": TagsProp(),
        "COMMENT": StringProp("COMMENT"),
    }

    def __init__(
        self,
        encryption: Union[None, str, dict, EncryptionType] = None,
        directory=None,
        file_format: Union[None, str, FileFormat] = None,
        copy_options: Optional[dict] = None,
        tags: List[Tuple[str, str]] = [],
        comment: Optional[str] = None,
        **kwargs,
    ):
        super().__init__(stage_type=StageType.INTERNAL, **kwargs)

        # Default
        # encryption={type:"SNOWFLAKE_FULL"}

        self.encryption = None
        if isinstance(encryption, EncryptionType):
            self.encryption = {"type": encryption}
        if isinstance(encryption, str):
            self.encryption = {"type": EncryptionType.parse(encryption)}
        elif isinstance(encryption, dict):
            self.encryption = {"type": EncryptionType.parse(encryption.get("type") or encryption.get("TYPE"))}

        self.directory = directory
        self.file_format = FileFormat.all[file_format] if isinstance(file_format, str) else file_format
        self.copy_options = copy_options
        self.tags = tags
        self.comment = comment


class ExternalStage(Stage):
    props = {
        "URL": StringProp("URL"),
        "DIRECTORY": PropSet(
            "DIRECTORY", {"ENABLE": BoolProp("ENABLE"), "REFRESH_ON_CREATE": BoolProp("REFRESH_ON_CREATE")}
        ),
        "FILE_FORMAT": [
            IdentifierProp("FILE_FORMAT"),
            PropSet("FILE_FORMAT", {"FORMAT_NAME": IdentifierProp("FORMAT_NAME")}),
            # AnonFileFormatProp("FILE_FORMAT"),
        ],
        "COPY_OPTIONS": _copy_options,
        "TAGS": TagsProp(),
        "COMMENT": StringProp("COMMENT"),
    }

    def __init__(self, file_format: Union[None, str, FileFormat] = None, **kwargs):
        super().__init__(stage_type=StageType.EXTERNAL, **kwargs)
        self.file_format = FileFormat.all[file_format] if isinstance(file_format, str) else file_format
