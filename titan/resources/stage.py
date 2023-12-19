from abc import ABC
from typing import Dict, Union, Any

from ..enums import ParseableEnum
from ..parse import _resolve_resource_class
from .base import Resource, SchemaScoped, _fix_class_documentation
from ..props import (
    BoolProp,
    EnumProp,
    IntProp,
    Props,
    StringProp,
    TagsProp,
    PropSet,
)
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
copy_options = Props(
    on_error=StringProp("on_error", alt_tokens=["CONTINUE", "SKIP_FILE", "ABORT_STATEMENT"]),
    size_limit=IntProp("size_limit"),
    purge=BoolProp("purge"),
    return_failed_only=BoolProp("return_failed_only"),
    match_by_column_name=StringProp("match_by_column_name", alt_tokens=["CASE_SENSITIVE", "CASE_INSENSITIVE", "NONE"]),
    enforce_length=BoolProp("enforce_length"),
    truncatecolumns=BoolProp("truncatecolumns"),
    force=BoolProp("force"),
)


@_fix_class_documentation
class InternalStage(SchemaScoped, Resource):
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

    resource_type = "STAGE"
    props = Props(
        encryption=PropSet(
            "encryption",
            Props(type=EnumProp("type", [EncryptionType.SNOWFLAKE_FULL, EncryptionType.SNOWFLAKE_SSE])),
        ),
        directory=PropSet(
            "directory", Props(enable=BoolProp("ENABLE"), refresh_on_create=BoolProp("REFRESH_ON_CREATE"))
        ),
        file_format=FileFormatProp("file_format"),
        copy_options=PropSet("copy_options", copy_options),
        tags=TagsProp(),
        comment=StringProp("comment"),
    )

    name: str
    owner: str = "SYSADMIN"
    type: StageType = StageType.INTERNAL
    encryption: Dict[str, EncryptionType] = None
    file_format: Union[str, dict] = None
    directory: Dict[str, bool] = None
    copy_options: Dict[str, Any] = None
    tags: Dict[str, str] = None
    comment: str = None


@_fix_class_documentation
class ExternalStage(SchemaScoped, Resource):
    """
    -- External stage
    CREATE [ OR REPLACE ] [ { TEMP | TEMPORARY } ] STAGE [ IF NOT EXISTS ] <external_stage_name>
        externalStageParams
        directoryTableParams
      [ FILE_FORMAT = ( { FORMAT_NAME = '<file_format_name>'
                         | TYPE = { CSV | JSON | AVRO | ORC | PARQUET | XML } [ formatTypeOptions ] } ) ]
      [ COPY_OPTIONS = ( copyOptions ) ]
      [ [ WITH ] TAG ( <tag_name> = '<tag_value>' [ , <tag_name> = '<tag_value>' , ... ] ) ]
      [ COMMENT = '<string_literal>' ]

    externalStageParams (for Amazon S3) ::=
      URL = { 's3://<bucket>[/<path>/]' | 's3gov://<bucket>[/<path>/]' }

      [ { STORAGE_INTEGRATION = <integration_name> }
        | { CREDENTIALS = ( {  { AWS_KEY_ID = '<string>' AWS_SECRET_KEY = '<string>' [ AWS_TOKEN = '<string>' ] } | AWS_ROLE = '<string>'  } ) ) } ]
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

    resource_type = "STAGE"
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
        copy_options=PropSet("copy_options", copy_options),
        tags=TagsProp(),
        comment=StringProp("comment"),
    )

    name: str
    owner: str = "SYSADMIN"
    type: StageType = StageType.EXTERNAL
    url: str
    directory: Dict[str, bool] = None
    storage_integration: str = None
    encryption: Dict[str, EncryptionType] = None
    file_format: Union[str, dict] = None
    directory: Dict[str, bool] = None
    copy_options: Dict[str, Any] = None
    tags: Dict[str, str] = None
    comment: str = None


StageTypeMap = {
    StageType.INTERNAL: InternalStage,
    StageType.EXTERNAL: ExternalStage,
}


class Stage(Resource, ABC):
    def __new__(cls, type: Union[str, StageType], **kwargs) -> Union[InternalStage, ExternalStage]:
        stage_type = StageType.parse(type)
        stage_cls = StageTypeMap[stage_type]
        return stage_cls(type=stage_type, **kwargs)

    @classmethod
    def from_sql(cls, sql):
        resource_cls = Resource.classes[_resolve_resource_class(sql)]
        return resource_cls.from_sql(sql)
