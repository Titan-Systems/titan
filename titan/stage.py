import re

from typing import List, Optional, Tuple

from .props import StringProp, TagsProp, Identifier
from .resource import SchemaLevelResource


class Stage(SchemaLevelResource):
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

    """

    props = {
        "URL": StringProp("URL"),
        "TAGS": TagsProp(),
        "COMMENT": StringProp("COMMENT"),
    }

    create_statement = re.compile(
        rf"""
            CREATE\s+
            (?:OR\s+REPLACE\s+)?
            (?:(?:TEMP|TEMPORARY)\s+)?
            STAGE\s+
            (?:IF\s+NOT\s+EXISTS\s+)?
            ({Identifier.pattern})
        """,
        re.VERBOSE | re.IGNORECASE,
    )

    ownable = True

    def __init__(
        self, url: Optional[str] = None, tags: List[Tuple[str, str]] = [], comment: Optional[str] = None, **kwargs
    ):
        super().__init__(**kwargs)
        self.url = url
        self.tags = tags
        self.comment = comment
        self.stage_type = "EXTERNAL" if url else "INTERNAL"
        self.hooks = {"on_file_added": None}

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
