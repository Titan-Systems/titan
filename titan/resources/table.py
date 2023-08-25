from typing import List, Dict

from pydantic import field_validator

from .base import Resource, SchemaScoped
from ..enums import SchemaPriv, TablePriv
from ..privs import Privs
from ..props import (
    BoolProp,
    ColumnsProp,
    FlagProp,
    IntProp,
    Props,
    PropSet,
    StringProp,
    StringListProp,
    TagsProp,
)
from .column import T_Column
from .stage import InternalStage, copy_options
from .file_format import FileFormatProp


class Table(Resource, SchemaScoped):
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

    resource_type = "TABLE"
    lifecycle_privs = Privs(
        create=SchemaPriv.CREATE_TABLE,
        read=TablePriv.SELECT,
        write=[TablePriv.INSERT, TablePriv.UPDATE, TablePriv.DELETE, TablePriv.TRUNCATE],
        delete=TablePriv.OWNERSHIP,
    )
    props = Props(
        columns=ColumnsProp(),
        volatile=FlagProp("volatile"),
        transient=FlagProp("transient"),
        cluster_by=StringListProp("cluster by"),
        enable_schema_evolution=BoolProp("enable_schema_evolution"),
        stage_file_format=FileFormatProp("stage_file_format"),
        stage_copy_options=PropSet("stage_copy_options", copy_options),
        data_retention_time_in_days=IntProp("data_retention_time_in_days"),
        max_data_extension_time_in_days=IntProp("max_data_extension_time_in_days"),
        change_tracking=BoolProp("change_tracking"),
        default_ddl_collation=StringProp("default_ddl_collation"),
        copy_grants=FlagProp("copy grants"),
        tags=TagsProp(),
        comment=StringProp("comment"),
    )

    name: str
    owner: str = "SYSADMIN"
    columns: List[T_Column]
    volatile: bool = False
    transient: bool = False
    cluster_by: List[str] = []
    enable_schema_evolution: bool = False
    data_retention_time_in_days: int = None
    max_data_extension_time_in_days: int = None
    change_tracking: bool = False
    default_ddl_collation: str = None
    copy_grants: bool = False
    tags: Dict[str, str] = None
    comment: str = None

    _table_stage: InternalStage

    def model_post_init(self, ctx):
        super().model_post_init(ctx)
        self._table_stage = InternalStage(name=f"@%{self.name}", implicit=True)
        self._table_stage.parent = self.parent

    @property
    def table_stage(self):
        return self._table_stage

    @field_validator("columns")
    @classmethod
    def validate_columns(cls, columns):
        if isinstance(columns, list):
            assert len(columns) > 0, "columns must not be empty"
        return columns

    @property
    def select_star_sql(self):
        return f"SELECT * FROM {self.fully_qualified_name}"

    # # https://github.com/python/mypy/issues/5936
    # @SchemaLevelResource.schema.setter  # type: ignore[attr-defined]
    # def schema(self, schema_: Optional["Schema"]):
    #     self._schema = schema_
    #     if self._schema is not None:
    #         self.requires(self._schema)
    #         self.table_stage.schema = self._schema

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