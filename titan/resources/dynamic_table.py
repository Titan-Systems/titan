from dataclasses import dataclass

from ..enums import ParseableEnum, ResourceType
from ..props import (
    ColumnNamesProp,
    EnumProp,
    IdentifierProp,
    Props,
    QueryProp,
    StringProp,
    TagsProp,
)
from ..resource_name import ResourceName
from ..role_ref import RoleRef
from ..scope import SchemaScope, TableScope
from .resource import NamedResource, Resource, ResourceSpec
from .tag import TaggableResource
from .warehouse import Warehouse


class RefreshMode(ParseableEnum):
    AUTO = "AUTO"
    FULL = "FULL"
    INCREMENTAL = "INCREMENTAL"


class InitializeBehavior(ParseableEnum):
    ON_CREATE = "ON_CREATE"
    ON_SCHEDULE = "ON_SCHEDULE"


@dataclass(unsafe_hash=True)
class _DynamicTableColumn(ResourceSpec):
    name: str
    comment: str = None


class DynamicTableColumn(Resource):
    resource_type = ResourceType.COLUMN
    props = Props(
        comment=StringProp("comment", eq=False),
    )
    scope = TableScope()
    spec = _DynamicTableColumn
    serialize_inline = True

    def __init__(
        self,
        name: str,
        comment: str = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._data: _DynamicTableColumn = _DynamicTableColumn(
            name,
            comment=comment,
        )


@dataclass(unsafe_hash=True)
class _DynamicTable(ResourceSpec):
    name: ResourceName
    columns: list[DynamicTableColumn]
    target_lag: str
    warehouse: Warehouse
    as_: str
    refresh_mode: RefreshMode = RefreshMode.AUTO
    initialize: InitializeBehavior = InitializeBehavior.ON_CREATE
    comment: str = None
    owner: RoleRef = "SYSADMIN"


class DynamicTable(NamedResource, TaggableResource, Resource):
    """
    Description:
        Represents a dynamic table in Snowflake, which can be configured to refresh automatically,
        fully, or incrementally, and initialized on creation or on a schedule.

    Snowflake Docs:
        https://docs.snowflake.com/en/sql-reference/sql/create-dynamic-table

    Fields:
        name (string, required): The name of the dynamic table.
        columns (list, required): A list of dicts defining the structure of the table.
        target_lag (string): The acceptable lag (delay) for data in the table. Defaults to "DOWNSTREAM".
        warehouse (string or Warehouse, required): The warehouse where the table is stored.
        as_ (string, required): The query used to populate the table.
        refresh_mode (string or RefreshMode): The mode of refreshing the table (AUTO, FULL, INCREMENTAL).
        initialize (string or InitializeBehavior): The behavior when the table is initialized (ON_CREATE, ON_SCHEDULE).
        comment (string): An optional comment for the table.
        owner (string or Role): The owner of the table. Defaults to "SYSADMIN".

    Python:

        ```python
        dynamic_table = DynamicTable(
            name="some_dynamic_table",
            columns=[{"name": "id"}, {"name": "data"}],
            target_lag="1 HOUR",
            warehouse="some_warehouse",
            refresh_mode="AUTO",
            initialize="ON_CREATE",
            as_="SELECT id, data FROM source_table",
            comment="This is a sample dynamic table",
            owner="SYSADMIN"
        )
        ```

    Yaml:

        ```yaml
        dynamic_table:
          name: some_dynamic_table
          columns:
            - name: id
            - name: data
          target_lag: "1 HOUR"
          warehouse: some_warehouse
          refresh_mode: AUTO
          initialize: ON_CREATE
          as_: "SELECT id, data FROM source_table"
          comment: "This is a sample dynamic table"
          owner: SYSADMIN
        ```
    """

    resource_type = ResourceType.DYNAMIC_TABLE
    props = Props(
        columns=ColumnNamesProp(),
        target_lag=StringProp("target_lag", alt_tokens=["DOWNSTREAM"]),
        warehouse=IdentifierProp("warehouse"),
        refresh_mode=EnumProp("refresh_mode", RefreshMode),
        initialize=EnumProp("initialize", InitializeBehavior),
        comment=StringProp("comment"),
        as_=QueryProp("as"),
        tags=TagsProp(),
    )
    scope = SchemaScope()
    spec = _DynamicTable

    def __init__(
        self,
        name: str,
        columns: list[dict],
        target_lag: str,
        warehouse: str,
        as_: str,
        refresh_mode: RefreshMode = RefreshMode.AUTO,
        initialize: InitializeBehavior = InitializeBehavior.ON_CREATE,
        comment: str = None,
        owner: str = "SYSADMIN",
        tags: dict[str, str] = None,
        **kwargs,
    ):
        super().__init__(name, **kwargs)
        self._data: _DynamicTable = _DynamicTable(
            name=self._name,
            columns=columns,
            target_lag=target_lag,
            warehouse=warehouse,
            refresh_mode=refresh_mode,
            initialize=initialize,
            as_=as_,
            comment=comment,
            owner=owner,
        )
        self.set_tags(tags)
