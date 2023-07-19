import re

from typing import Dict, Union

from .props import (
    BoolProp,
    EnumProp,
    IntProp,
    ParseableEnum,
    Props,
    StringProp,
    TagsProp,
    PropSet,
)


from .resource import Resource, Namespace
from .file_format import FileFormatProp


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
    resource_type = "STAGE"
    namespace = Namespace.SCHEMA

    name: str
    owner: str = None

    @classmethod
    def _resolve_class(cls, resource_type: str, props_sql: str):
        if re.search(r"URL\s*=", props_sql, re.IGNORECASE):
            return ExternalStage
        else:
            return InternalStage

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


"""
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
_copy_options = PropSet(
    "COPY_OPTIONS",
    Props(
        on_error=StringProp("on_error"),
        size_limit=IntProp("size_limit"),
        purge=BoolProp("purge"),
        return_failed_only=BoolProp("return_failed_only"),
        match_by_column_name=StringProp(
            "match_by_column_name", alt_tokens=["CASE_SENSITIVE", "CASE_INSENSITIVE", "NONE"]
        ),
        enforce_length=BoolProp("enforce_length"),
        truncatecolumns=BoolProp("truncatecolumns"),
        force=BoolProp("force"),
    ),
)


class InternalStage(Stage):
    """
    -- Internal stage
    CREATE [ OR REPLACE ] [ { TEMP | TEMPORARY } ] STAGE [ IF NOT EXISTS ] <internal_stage_name>
        internalStageParams
        directoryTableParams
      [ FILE_FORMAT = ( { FORMAT_NAME = '<file_format_name>'
                         | TYPE = { CSV | JSON | AVRO | ORC | PARQUET | XML } [ formatTypeOptions ] } ) ]
      [ COPY_OPTIONS = ( copyOptions ) ]
      [ [ WITH ] TAG ( <tag_name> = '<tag_value>' [ , <tag_name> = '<tag_value>' , ... ] ) ]
      [ COMMENT = '<string_literal>' ]

    internalStageParams ::=
      [ ENCRYPTION = (TYPE = 'SNOWFLAKE_FULL' | TYPE = 'SNOWFLAKE_SSE') ]

    directoryTableParams (for internal stages) ::=
      [ DIRECTORY = ( ENABLE = { TRUE | FALSE }
                      [ REFRESH_ON_CREATE =  { TRUE | FALSE } ] ) ]
    """

    props = Props(
        encryption=PropSet(
            "encryption",
            Props(type=EnumProp("type", [EncryptionType.SNOWFLAKE_FULL, EncryptionType.SNOWFLAKE_SSE])),
        ),
        directory=PropSet(
            "directory", Props(enable=BoolProp("ENABLE"), refresh_on_create=BoolProp("REFRESH_ON_CREATE"))
        ),
        file_format=FileFormatProp("file_format"),
        copy_options=_copy_options,
        tags=TagsProp(),
        comment=StringProp("comment"),
    )

    name: str
    owner: str = None
    encryption: Dict[str, EncryptionType] = None
    file_format: Union[str, dict] = None
    directory: Dict[str, bool] = None
    copy_options: Dict[str, str] = None
    tags: Dict[str, str] = None
    comment: str = None


class ExternalStage(Stage):
    """
    -- External stage
    CREATE [ OR REPLACE ] [ { TEMP | TEMPORARY } ] STAGE [ IF NOT EXISTS ] <external_stage_name>
        externalStageParams
        directoryTableParams
      [ FILE_FORMAT = ( { FORMAT_NAME = '<file_format_name>' | TYPE = { CSV | JSON | AVRO | ORC | PARQUET | XML } [ formatTypeOptions ] } ) ]
      [ COPY_OPTIONS = ( copyOptions ) ]
      [ [ WITH ] TAG ( <tag_name> = '<tag_value>' [ , <tag_name> = '<tag_value>' , ... ] ) ]
      [ COMMENT = '<string_literal>' ]

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

    directoryTableParams (for internal stages) ::=
      [ DIRECTORY = ( ENABLE = { TRUE | FALSE }
                      [ REFRESH_ON_CREATE =  { TRUE | FALSE } ] ) ]
    """

    props = Props(
        url=StringProp("url"),
        storage_integration=StringProp("storage_integration"),
        encryption=PropSet(
            "encryption",
            Props(type=EnumProp("type", [EncryptionType.SNOWFLAKE_FULL, EncryptionType.SNOWFLAKE_SSE])),
        ),
        directory=PropSet(
            "directory", Props(enable=BoolProp("ENABLE"), refresh_on_create=BoolProp("REFRESH_ON_CREATE"))
        ),
        file_format=FileFormatProp("file_format"),
        copy_options=_copy_options,
        tags=TagsProp(),
        comment=StringProp("comment"),
    )

    name: str
    owner: str = None
    url: str
    directory: Dict[str, bool] = None
    storage_integration: str = None
    encryption: Dict[str, EncryptionType] = None
    file_format: Union[str, dict] = None
    directory: Dict[str, bool] = None
    copy_options: Dict[str, str] = None
    tags: Dict[str, str] = None
    comment: str = None

    # props = {
    #     "URL": StringProp("URL"),
    #     "DIRECTORY": PropSet(
    #         "DIRECTORY", {"ENABLE": BoolProp("ENABLE"), "REFRESH_ON_CREATE": BoolProp("REFRESH_ON_CREATE")}
    #     ),
    #     "FILE_FORMAT": [
    #         IdentifierProp("FILE_FORMAT"),
    #         PropSet("FILE_FORMAT", {"FORMAT_NAME": IdentifierProp("FORMAT_NAME")}),
    #         # AnonFileFormatProp("FILE_FORMAT"),
    #     ],
    #     "COPY_OPTIONS": _copy_options,
    #     "TAGS": TagsProp(),
    #     "COMMENT": StringProp("COMMENT"),
    # }

    # def __init__(self, file_format: Union[None, str, FileFormat] = None, **kwargs):
    #     super().__init__(stage_type=StageType.EXTERNAL, **kwargs)
    #     self.file_format = FileFormat.all[file_format] if isinstance(file_format, str) else file_format
