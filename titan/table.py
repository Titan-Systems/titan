import re

from typing import List, Tuple, Optional, TYPE_CHECKING

from .parseable_enum import ParseableEnum
from .props import (
    BoolProp,
    FlagProp,
    Identifier,
    IdentifierListProp,
    IdentifierProp,
    IntProp,
    PropSet,
    StringProp,
    TagsProp,
    FileFormatProp,
)
from .resource import SchemaLevelResource

if TYPE_CHECKING:
    from .schema import Schema

from .stage import InternalStage


class Table(SchemaLevelResource):
    """
    CREATE [ OR REPLACE ]
      [ { [ { LOCAL | GLOBAL } ] TEMP | TEMPORARY | VOLATILE | TRANSIENT } ]
      TABLE [ IF NOT EXISTS ] <table_name>
      ( ... )
          [ CLUSTER BY ( <expr> [ , <expr> , ... ] ) ]
          [ ENABLE_SCHEMA_EVOLUTION = { TRUE | FALSE } ]
          [ STAGE_FILE_FORMAT = ( { FORMAT_NAME = '<file_format_name>'
                                   | TYPE = { CSV | JSON | AVRO | ORC | PARQUET | XML } [ formatTypeOptions ] } ) ]
          [ STAGE_COPY_OPTIONS = ( copyOptions ) ]
          [ DATA_RETENTION_TIME_IN_DAYS = <integer> ]
          [ MAX_DATA_EXTENSION_TIME_IN_DAYS = <integer> ]
          [ CHANGE_TRACKING = { TRUE | FALSE } ]
          [ DEFAULT_DDL_COLLATION = '<collation_specification>' ]
          [ COPY GRANTS ]
          [ [ WITH ] ROW ACCESS POLICY <policy_name> ON ( <col_name> [ , <col_name> ... ] ) ]
          [ [ WITH ] TAG ( <tag_name> = '<tag_value>' [ , <tag_name> = '<tag_value>' , ... ] ) ]
          [ COMMENT = '<string_literal>' ]
    """

    props = {
        "cluster_by": IdentifierListProp("CLUSTER BY"),
        "enable_schema_evolution": BoolProp("ENABLE_SCHEMA_EVOLUTION"),
        "stage_file_format": FileFormatProp("STAGE_FILE_FORMAT"),
        # STAGE_COPY_OPTIONS
        "data_retention_time_in_days": IntProp("DATA_RETENTION_TIME_IN_DAYS"),
        "max_data_extension_time_in_days": IntProp("MAX_DATA_EXTENSION_TIME_IN_DAYS"),
        "change_tracking": BoolProp("CHANGE_TRACKING"),
        "default_ddl_collation": StringProp("DEFAULT_DDL_COLLATION"),
        "copy_grants": FlagProp("COPY GRANTS"),
        "tags": TagsProp(),
        "comment": StringProp("COMMENT"),
    }

    create_statement = re.compile(
        rf"""
            CREATE\s+
            (?:OR\s+REPLACE\s+)?
            (?:(?:LOCAL|GLOBAL)\s+)?
            (?:(?:TEMP|TEMPORARY|VOLATILE|TRANSIENT)\s+)?
            TABLE\s+
            (?:IF\s+NOT\s+EXISTS\s+)?
            ({Identifier.pattern})\s*
            \([\S\s]*\)
        """,
        re.VERBOSE | re.IGNORECASE,
    )

    ownable = True

    def __init__(
        self,
        name: str,
        cluster_by: List[str] = [],
        enable_schema_evolution: Optional[bool] = None,
        data_retention_time_in_days: Optional[int] = None,
        max_data_extension_time_in_days: Optional[int] = None,
        change_tracking: Optional[bool] = None,
        default_ddl_collation: Optional[str] = None,
        copy_grants: Optional[bool] = None,
        tags: List[Tuple[str, str]] = [],
        comment: Optional[str] = None,
        autoload: Optional[bool] = False,
        **kwargs,
    ):
        super().__init__(name, **kwargs)

        self.cluster_by = cluster_by
        self.enable_schema_evolution = enable_schema_evolution
        self.data_retention_time_in_days = data_retention_time_in_days
        self.max_data_extension_time_in_days = max_data_extension_time_in_days
        self.change_tracking = change_tracking
        self.default_ddl_collation = default_ddl_collation
        self.copy_grants = copy_grants
        self.tags = tags
        self.comment = comment

        # TODO: make this a changeable property that registers/deregisters the pipe when the flag is flipped
        self.autoload = autoload

        self.table_stage = InternalStage(name=f"@%{self.name}", implicit=True)
        self.table_stage.requires(self)
        if self.schema:
            self.table_stage.schema = self.schema

    @property
    def create_sql(self):
        props = self.props_sql()
        return f"CREATE TABLE {self.fully_qualified_name} () {props}"

    # https://github.com/python/mypy/issues/5936
    @SchemaLevelResource.schema.setter  # type: ignore[attr-defined]
    def schema(self, schema_: Optional["Schema"]):
        self._schema = schema_
        if self._schema is not None:
            self.requires(self._schema)
            self.table_stage.schema = self._schema

    # def create(self, session):
    #     super().create(session)
    #     if self.autoload:
    #         raise NotImplementedError
    # Needs a refactor via dependencies
    # # Does this need to be a pipe we refresh, or should we just call the COPY INTO command each time?
    # pipe = Pipe(
    #     sql=rf"""
    #     CREATE PIPE {self.name}_autoload_pipe
    #         AS
    #         COPY INTO {self.name}
    #         FROM {self.table_stage}
    #         FILE_FORMAT = (
    #             TYPE = CSV
    #             SKIP_HEADER = 1
    #             COMPRESSION = GZIP
    #             FIELD_OPTIONALLY_ENCLOSED_BY = '\042'
    #             NULL_IF = '\N'
    #             NULL_IF = 'NULL'
    #         )
    #     """,
    # )
    # pipe.create(session)


class ColumnType(ParseableEnum):
    NUMBER = "NUMBER"
    DECIMAL = "DECIMAL"
    NUMERIC = "NUMERIC"
    INT = "INT"
    INTEGER = "INTEGER"
    BIGINT = "BIGINT"
    SMALLINT = "SMALLINT"
    TINYINT = "TINYINT"
    BYTEINT = "BYTEINT"
    FLOAT = "FLOAT"
    FLOAT4 = "FLOAT4"
    FLOAT8 = "FLOAT8"
    DOUBLE = "DOUBLE"
    DOUBLE_PRECISION = "DOUBLE PRECISION"
    REAL = "REAL"
    VARCHAR = "VARCHAR"
    CHAR = "CHAR"
    CHARACTER = "CHARACTER"
    NCHAR = "NCHAR"
    STRING = "STRING"
    TEXT = "TEXT"
    NVARCHAR = "NVARCHAR"
    NVARCHAR2 = "NVARCHAR2"
    CHAR_VARYING = "CHAR VARYING"
    NCHAR_VARYING = "NCHAR VARYING"
    BINARY = "BINARY"
    VARBINARY = "VARBINARY"
    BOOLEAN = "BOOLEAN"
    DATE = "DATE"
    DATETIME = "DATETIME"
    TIME = "TIME"
    TIMESTAMP = "TIMESTAMP"
    TIMESTAMP_LTZ = "TIMESTAMP_LTZ"
    TIMESTAMP_NTZ = "TIMESTAMP_NTZ"
    TIMESTAMP_TZ = "TIMESTAMP_TZ"
    ARRAY = "ARRAY"
    OBJECT = "OBJECT"
    VARIANT = "VARIANT"
    GEOGRAPHY = "GEOGRAPHY"
    GEOMETRY = "GEOMETRY"


class Column:
    """
    <col_name> <col_type>
      [ COLLATE '<collation_specification>' ]
      [ COMMENT '<string_literal>' ]
      [ { DEFAULT <expr>
        | { AUTOINCREMENT | IDENTITY } [ { ( <start_num> , <step_num> ) | START <num> INCREMENT <num> } ] } ]
      [ NOT NULL ]
      [ [ WITH ] MASKING POLICY <policy_name> [ USING ( <col_name> , <cond_col1> , ... ) ] ]
      [ [ WITH ] TAG ( <tag_name> = '<tag_value>' [ , <tag_name> = '<tag_value>' , ... ] ) ]
      [ inlineConstraint ]
      [ , <col_name> <col_type> [ ... ] ]
      [ , outoflineConstraint ]
      [ , ... ]

    inlineConstraint ::=
      [ CONSTRAINT <constraint_name> ]
      { UNIQUE | PRIMARY KEY | { [ FOREIGN KEY ] REFERENCES <ref_table_name> [ ( <ref_col_name> ) ] } }
      [ <constraint_properties> ]

    outoflineConstraint ::=
      [ CONSTRAINT <constraint_name> ]
      {
         UNIQUE [ ( <col_name> [ , <col_name> , ... ] ) ]
       | PRIMARY KEY [ ( <col_name> [ , <col_name> , ... ] ) ]
       | [ FOREIGN KEY ] [ ( <col_name> [ , <col_name> , ... ] ) ]
                         REFERENCES <ref_table_name> [ ( <ref_col_name> [ , <ref_col_name> , ... ] ) ]
      }
      [ <constraint_properties> ]
    """

    def __init__(
        self,
        name: str,
        col_type: str,
        collate: Optional[str] = None,
        comment: Optional[str] = None,
        default=None,
        not_null: Optional[bool] = None,
        masking_policy=None,
        tags: List[Tuple[str, str]] = [],
        **kwargs,
    ) -> None:
        self.name = name
        self.col_type = col_type
        self.collate = collate
        self.comment = comment
        self.default = default
        self.not_null = not_null
        self.masking_policy = masking_policy
        self.tags = tags


class Columns:
    pass


# Table("raw_data", columns=)
