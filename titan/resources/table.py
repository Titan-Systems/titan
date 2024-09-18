from dataclasses import dataclass, field

# from .stage import InternalStage, copy_options
from ..enums import ResourceType
from ..parse import _parse_create_header, _parse_props, _parse_table_schema
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
from ..role_ref import RoleRef
from ..scope import SchemaScope
from .column import Column
from .resource import NamedResource, Resource, ResourceName, ResourceSpec
from .tag import TaggableResource


@dataclass(unsafe_hash=True)
class _Table(ResourceSpec):
    name: ResourceName
    # TODO: allow columns to be specified as SQL
    columns: list[Column]
    constraints: list[str] = None
    transient: bool = False
    cluster_by: list[str] = None
    enable_schema_evolution: bool = False
    data_retention_time_in_days: int = None
    max_data_extension_time_in_days: int = None
    change_tracking: bool = False
    default_ddl_collation: str = None
    copy_grants: bool = field(default=None, metadata={"fetchable": False})
    row_access_policy: dict[str, list] = None
    owner: RoleRef = "SYSADMIN"
    comment: str = None

    def __post_init__(self):
        super().__post_init__()
        if self.columns is None:
            raise ValueError("columns can't be None")
        if len(self.columns) == 0:
            raise ValueError("columns can't be empty")


class Table(NamedResource, TaggableResource, Resource):
    """
    Description:
        A table in Snowflake.

    Snowflake Docs:
        https://docs.snowflake.com/en/sql-reference/sql/create-table

    Fields:
        name (string, required): The name of the table.
        columns (list, required): The columns of the table.
        constraints (list): The constraints of the table.
        transient (bool): Whether the table is transient.
        cluster_by (list): The clustering keys for the table.
        enable_schema_evolution (bool): Whether schema evolution is enabled. Defaults to False.
        data_retention_time_in_days (int): The data retention time in days.
        max_data_extension_time_in_days (int): The maximum data extension time in days.
        change_tracking (bool): Whether change tracking is enabled. Defaults to False.
        default_ddl_collation (string): The default DDL collation.
        copy_grants (bool): Whether to copy grants. Defaults to False.
        row_access_policy (dict): The row access policy.
        tags (dict): The tags for the table.
        owner (string or Role): The owner role of the table. Defaults to SYSADMIN.
        comment (string): A comment for the table.

    Python:

        ```python
        table = Table(
            name="some_table",
            columns=[{"name": "col1", "data_type": "STRING"}],
            owner="SYSADMIN",
        )
        ```

    Yaml:

        ```yaml
        tables:
          - name: some_table
            columns:
              - name: col1
                data_type: STRING
            owner: SYSADMIN
        ```
    """

    resource_type = ResourceType.TABLE
    props = Props(
        transient=FlagProp("transient"),
        columns=SchemaProp(),
        cluster_by=IdentifierListProp("cluster by", eq=False, parens=True),
        enable_schema_evolution=BoolProp("enable_schema_evolution"),
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
        transient: bool = False,
        cluster_by: list[str] = None,
        enable_schema_evolution: bool = False,
        data_retention_time_in_days: int = None,
        max_data_extension_time_in_days: int = None,
        change_tracking: bool = False,
        default_ddl_collation: str = None,
        copy_grants: bool = None,
        row_access_policy: dict[str, list] = None,
        tags: dict[str, str] = None,
        owner: str = "SYSADMIN",
        comment: str = None,
        **kwargs,
    ):

        if "lifecycle" not in kwargs:
            lifecycle = {
                "ignore_changes": "columns",
            }
            kwargs["lifecycle"] = lifecycle

        super().__init__(name, **kwargs)
        self._data = _Table(
            name=self._name,
            columns=columns,
            constraints=constraints,
            transient=transient,
            cluster_by=cluster_by,
            enable_schema_evolution=enable_schema_evolution,
            data_retention_time_in_days=data_retention_time_in_days,
            max_data_extension_time_in_days=max_data_extension_time_in_days,
            change_tracking=change_tracking,
            default_ddl_collation=default_ddl_collation,
            copy_grants=copy_grants,
            row_access_policy=row_access_policy,
            owner=owner,
            comment=comment,
        )
        self._table_stage = None
        self.set_tags(tags)
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

        identifier, remainder = _parse_create_header(sql, cls.resource_type, cls.scope)
        table_schema, remainder = _parse_table_schema(remainder)
        props = _parse_props(cls.props, remainder)
        return cls(**identifier, **table_schema, **props)

    @property
    def table_stage(self):
        return self._table_stage


# @dataclass(unsafe_hash=True)
# class _CreateTableAsSelect(ResourceSpec):
#     name: str
#     as_: str = field(default=None, metadata={"triggers_replacement": True})
#     columns: list[Column] = field(default=None, metadata={"triggers_replacement": True})
#     cluster_by: list[str] = None
#     copy_grants: bool = False
#     row_access_policy: dict[str, list] = None
#     owner: Role = "SYSADMIN"
#     comment: str = None

#     def __post_init__(self):
#         super().__post_init__()
#         if self.as_ is None:
#             raise ValueError("as can't be None")


# class CreateTableAsSelect(Resource):
#     resource_type = ResourceType.TABLE
#     props = Props(
#         columns=SchemaProp(),
#         cluster_by=IdentifierListProp("cluster by", eq=False, parens=True),
#         copy_grants=FlagProp("copy grants"),
#         as_=QueryProp("as"),
#     )
#     scope = SchemaScope()
#     spec = _CreateTableAsSelect

#     def __init__(
#         self,
#         name: str,
#         columns: list[Column] = None,
#         cluster_by: list[str] = None,
#         copy_grants: bool = False,
#         row_access_policy: dict[str, list] = None,
#         as_: str = None,
#         owner: str = "SYSADMIN",
#         **kwargs,
#     ):
#         super().__init__(**kwargs)
#         self._data = _CreateTableAsSelect(
#             name=name,
#             as_=as_,
#             columns=columns,
#             cluster_by=cluster_by,
#             copy_grants=copy_grants,
#             row_access_policy=row_access_policy,
#             owner=owner,
#         )

#     @classmethod
#     def from_sql(cls, sql):
#         """
#         CREATE [ OR REPLACE ] TABLE <table_name> [ ( <col_name> [ <col_type> ] , <col_name> [ <col_type> ] , ... ) ]
#         [ CLUSTER BY ( <expr> [ , <expr> , ... ] ) ]
#         [ COPY GRANTS ]
#         AS SELECT <query>
#         [ ... ]

#         """

#         identifier, remainder = _parse_create_header(sql, cls.resource_type, cls.scope)
#         table_schema, remainder = _parse_table_schema(remainder)
#         props = _parse_props(cls.props, remainder)
#         return cls(**identifier, **table_schema, **props)


# # there's no detectable difference between a table created with CTAS and a table created with a regular CREATE TABLE
# def _resolver(data: dict):
#     if "as_" in data:
#         return CreateTableAsSelect
#     return Table


# Resource.__resolvers__[ResourceType.TABLE] = _resolver
