from dataclasses import dataclass

from .resource import Resource, ResourceSpec
from .column import Column
from .warehouse import Warehouse
from ..enums import ParseableEnum, ResourceType
from ..scope import SchemaScope
from ..props import Props, EnumProp, StringProp, IdentifierProp, QueryProp, SchemaProp


class RefreshMode(ParseableEnum):
    AUTO = "AUTO"
    FULL = "FULL"
    INCREMENTAL = "INCREMENTAL"


class InitializeBehavior(ParseableEnum):
    ON_CREATE = "ON_CREATE"
    ON_SCHEDULE = "ON_SCHEDULE"


@dataclass
class _DynamicTable(ResourceSpec):
    name: str
    columns: list[Column]
    target_lag: str
    warehouse: Warehouse
    refresh_mode: RefreshMode
    initialize: str
    as_: str
    comment: str = None
    owner: str = "SYSADMIN"


class DynamicTable(Resource):
    resource_type = ResourceType.DYNAMIC_TABLE
    props = Props(
        columns=SchemaProp(),
        target_lag=StringProp("target_lag", alt_tokens=["DOWNSTREAM"]),
        warehouse=IdentifierProp("warehouse"),
        refresh_mode=EnumProp("refresh_mode", RefreshMode),
        initialize=EnumProp("initialize", InitializeBehavior),
        as_=QueryProp("as"),
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
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._data = _DynamicTable(
            name=name,
            columns=columns,
            target_lag=target_lag,
            warehouse=warehouse,
            refresh_mode=refresh_mode,
            initialize=initialize,
            as_=as_,
            comment=comment,
            owner=owner,
        )
