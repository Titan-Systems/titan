from dataclasses import dataclass

from .resource import Resource, ResourceSpec
from .column import Column

# from .stage import InternalStage, copy_options
from ..enums import ResourceType
from ..parse import _parse_create_header, _parse_props, _parse_table_schema
from ..scope import SchemaScope
from ..props import (
    BoolProp,
    FlagProp,
    IdentifierListProp,
    IntProp,
    Props,
    SchemaProp,
    StringProp,
    TagsProp,
)


@dataclass
class _Table(ResourceSpec):
    name: str
    # TODO: allow columns to be specified as SQL
    columns: list[Column]
    constraints: list[str] = None
    volatile: bool = False
    transient: bool = False
    cluster_by: list[str] = None
    enable_schema_evolution: bool = False
    data_retention_time_in_days: int = None
    max_data_extension_time_in_days: int = None
    change_tracking: bool = False
    default_ddl_collation: str = None
    copy_grants: bool = False
    row_access_policy: dict[str, list] = None
    tags: dict[str, str] = None
    owner: str = "SYSADMIN"
    comment: str = None

    def __post_init__(self):
        super().__post_init__()
        if self.columns is None:
            raise ValueError("columns can't be None")
        if len(self.columns) == 0:
            raise ValueError("columns can't be empty")


class Table(Resource):
    resource_type = ResourceType.TABLE
    props = Props(
        volatile=FlagProp("volatile"),
        transient=FlagProp("transient"),
        columns=SchemaProp(),
        cluster_by=IdentifierListProp("cluster by", eq=False, parens=True),
        enable_schema_evolution=BoolProp("enable_schema_evolution"),
        # stage_file_format=FileFormatProp("stage_file_format"),
        # stage_copy_options=PropSet("stage_copy_options", copy_options),
        data_retention_time_in_days=IntProp("data_retention_time_in_days"),
        max_data_extension_time_in_days=IntProp("max_data_extension_time_in_days"),
        change_tracking=BoolProp("change_tracking"),
        default_ddl_collation=StringProp("default_ddl_collation"),
        copy_grants=FlagProp("copy grants"),
        tags=TagsProp(),
        comment=StringProp("comment"),
    )
    scope = SchemaScope()
    spec = _Table

    def __init__(
        self,
        name: str,
        columns: list[Column],
        constraints: list[str] = None,
        volatile: bool = False,
        transient: bool = False,
        cluster_by: list[str] = None,
        enable_schema_evolution: bool = False,
        data_retention_time_in_days: int = None,
        max_data_extension_time_in_days: int = None,
        change_tracking: bool = False,
        default_ddl_collation: str = None,
        copy_grants: bool = False,
        row_access_policy: dict[str, list] = None,
        tags: dict[str, str] = None,
        owner: str = "SYSADMIN",
        comment: str = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._data = _Table(
            name=name,
            columns=columns,
            constraints=constraints,
            volatile=volatile,
            transient=transient,
            cluster_by=cluster_by,
            enable_schema_evolution=enable_schema_evolution,
            data_retention_time_in_days=data_retention_time_in_days,
            max_data_extension_time_in_days=max_data_extension_time_in_days,
            change_tracking=change_tracking,
            default_ddl_collation=default_ddl_collation,
            copy_grants=copy_grants,
            row_access_policy=row_access_policy,
            tags=tags,
            owner=owner,
            comment=comment,
        )
        self._table_stage = None
        # self._table_stage: InternalStage = InternalStage(name=f"@%{self.name}", implicit=True)
        # if self.schema:
        #     self._table_stage.schema = self.schema

    @classmethod
    def from_sql(cls, sql):
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

        identifier, remainder = _parse_create_header(sql, cls)
        table_schema, remainder = _parse_table_schema(remainder)
        # if "schema" in identifier:
        #     schema = Schema(name=identifier["schema"], stub=True)
        #     if "database" in identifier:
        #         schema.database = identifier["database"]
        #         del identifier["database"]
        #     identifier["schema"] = schema
        props = _parse_props(cls.props, remainder)
        return cls(**identifier, **table_schema, **props)

    @property
    def table_stage(self):
        return self._table_stage
