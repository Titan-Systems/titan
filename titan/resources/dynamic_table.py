from dataclasses import dataclass

from .resource import Resource, ResourceSpec, ResourceNameTrait
from .column import Column
from .role import Role
from .warehouse import Warehouse
from ..enums import ParseableEnum, ResourceType
from ..resource_name import ResourceName
from ..scope import SchemaScope
from ..props import (
    ArgsProp,
    EnumProp,
    IdentifierProp,
    Props,
    QueryProp,
    StringProp,
    TagsProp,
)


class RefreshMode(ParseableEnum):
    AUTO = "AUTO"
    FULL = "FULL"
    INCREMENTAL = "INCREMENTAL"


class InitializeBehavior(ParseableEnum):
    ON_CREATE = "ON_CREATE"
    ON_SCHEDULE = "ON_SCHEDULE"


@dataclass(unsafe_hash=True)
class _DynamicTable(ResourceSpec):
    name: ResourceName
    columns: list[Column]
    target_lag: str
    warehouse: Warehouse
    refresh_mode: RefreshMode
    initialize: str
    as_: str
    comment: str = None
    owner: Role = "SYSADMIN"
    tags: dict[str, str] = None


class DynamicTable(ResourceNameTrait, Resource):
    """
    Description:
        Represents a dynamic table in Snowflake, which can be configured to refresh automatically,
        fully, or incrementally, and initialized on creation or on a schedule.

    Snowflake Docs:
        https://docs.snowflake.com/en/sql-reference/sql/create-dynamic-table

    Fields:
        name (string, required): The name of the dynamic table.
        columns (list, required): A list of Column objects defining the structure of the table.
        target_lag (string): The acceptable lag (delay) for data in the table. Defaults to "DOWNSTREAM".
        warehouse (string or Warehouse, required): The warehouse where the table is stored.
        refresh_mode (string or RefreshMode, required): The mode of refreshing the table (AUTO, FULL, INCREMENTAL).
        initialize (string or InitializeBehavior, required): The behavior when the table is initialized (ON_CREATE, ON_SCHEDULE).
        as_ (string, required): The query used to populate the table.
        comment (string): An optional comment for the table.
        owner (string or Role): The owner of the table. Defaults to "SYSADMIN".

    Python:

        ```python
        dynamic_table = DynamicTable(
            name="some_dynamic_table",
            columns=[Column(name="id", type="int"), Column(name="data", type="string")],
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
              type: int
            - name: data
              type: string
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
        columns=ArgsProp(),
        target_lag=StringProp("target_lag", alt_tokens=["DOWNSTREAM"]),
        warehouse=IdentifierProp("warehouse"),
        refresh_mode=EnumProp("refresh_mode", RefreshMode),
        initialize=EnumProp("initialize", InitializeBehavior),
        as_=QueryProp("as"),
        tags=TagsProp(),
    )
    scope = SchemaScope()
    spec = _DynamicTable

    def __init__(
        self,
        name: str,
        columns: list[Column],
        target_lag: str,
        warehouse: str,
        refresh_mode: RefreshMode,
        initialize: InitializeBehavior,
        as_: str,
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
            tags=tags,
        )
