from dataclasses import dataclass

from .resource import Resource, ResourceSpec
from .resource_monitor import ResourceMonitor
from ..enums import ParseableEnum, ResourceType
from ..scope import AccountScope

from ..props import (
    BoolProp,
    EnumProp,
    IdentifierProp,
    IntProp,
    Props,
    StringProp,
    TagsProp,
)


class WarehouseType(ParseableEnum):
    STANDARD = "STANDARD"
    SNOWPARK_OPTIMIZED = "SNOWPARK-OPTIMIZED"


# TODO: add alias support, eg XSMALL = X-SMALL
class WarehouseSize(ParseableEnum):
    """
    Represents the size options for a warehouse.

    Available sizes:
    - XSMALL
    - SMALL
    - MEDIUM
    - LARGE
    - XLARGE
    - XXLARGE
    - XXXLARGE
    - X4LARGE
    - X5LARGE (AWS-only)
    - X6LARGE (AWS-only)
    """

    XSMALL = "XSMALL"
    SMALL = "SMALL"
    MEDIUM = "MEDIUM"
    LARGE = "LARGE"
    XLARGE = "XLARGE"
    XXLARGE = "XXLARGE"
    XXXLARGE = "XXXLARGE"
    X4LARGE = "X4LARGE"
    X5LARGE = "X5LARGE"
    X6LARGE = "X6LARGE"


class WarehouseScalingPolicy(ParseableEnum):
    STANDARD = "STANDARD"
    ECONOMY = "ECONOMY"


@dataclass
class _Warehouse(ResourceSpec):
    name: str
    owner: str = "SYSADMIN"
    warehouse_type: WarehouseType = "STANDARD"
    warehouse_size: WarehouseSize = None
    max_cluster_count: int = None
    min_cluster_count: int = None
    scaling_policy: WarehouseScalingPolicy = None
    auto_suspend: int = 600
    auto_resume: bool = True
    initially_suspended: bool = None
    resource_monitor: ResourceMonitor = None
    comment: str = None
    enable_query_acceleration: bool = None
    query_acceleration_max_scale_factor: int = None
    max_concurrency_level: int = 8
    statement_queued_timeout_in_seconds: int = 0
    statement_timeout_in_seconds: int = 172800
    tags: dict[str, str] = None


class Warehouse(Resource):
    """A virtual warehouse is a cluster of compute resources in Snowflake. It is used to execute SQL queries and load data."""

    resource_type = ResourceType.WAREHOUSE
    props = Props(
        _start_token="WITH",
        warehouse_type=EnumProp("warehouse_type", WarehouseType),
        warehouse_size=EnumProp("warehouse_size", WarehouseSize),
        max_cluster_count=IntProp("max_cluster_count"),
        min_cluster_count=IntProp("min_cluster_count"),
        scaling_policy=EnumProp("scaling_policy", WarehouseScalingPolicy),
        auto_suspend=IntProp("auto_suspend", alt_tokens=["NULL"]),
        auto_resume=BoolProp("auto_resume"),
        initially_suspended=BoolProp("initially_suspended"),
        resource_monitor=IdentifierProp("resource_monitor"),
        comment=StringProp("comment"),
        enable_query_acceleration=BoolProp("enable_query_acceleration"),
        query_acceleration_max_scale_factor=IntProp("query_acceleration_max_scale_factor"),
        max_concurrency_level=IntProp("max_concurrency_level"),
        statement_queued_timeout_in_seconds=IntProp("statement_queued_timeout_in_seconds"),
        statement_timeout_in_seconds=IntProp("statement_timeout_in_seconds"),
        tags=TagsProp(),
    )
    scope = AccountScope()
    spec = _Warehouse

    def __init__(
        self,
        name: str,
        owner: str = "SYSADMIN",
        warehouse_type: WarehouseType = "STANDARD",
        warehouse_size: WarehouseSize = None,
        max_cluster_count: int = None,
        min_cluster_count: int = None,
        scaling_policy: WarehouseScalingPolicy = None,
        auto_suspend: int = 600,
        auto_resume: bool = True,
        initially_suspended: bool = None,
        resource_monitor: ResourceMonitor = None,
        comment: str = None,
        enable_query_acceleration: bool = None,
        query_acceleration_max_scale_factor: int = None,
        max_concurrency_level: int = 8,
        statement_queued_timeout_in_seconds: int = 0,
        statement_timeout_in_seconds: int = 172800,
        tags: dict[str, str] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._data = _Warehouse(
            name=name,
            owner=owner,
            warehouse_type=warehouse_type,
            warehouse_size=warehouse_size,
            max_cluster_count=max_cluster_count,
            min_cluster_count=min_cluster_count,
            scaling_policy=scaling_policy,
            auto_suspend=auto_suspend,
            auto_resume=auto_resume,
            initially_suspended=initially_suspended,
            resource_monitor=resource_monitor,
            comment=comment,
            enable_query_acceleration=enable_query_acceleration,
            query_acceleration_max_scale_factor=query_acceleration_max_scale_factor,
            max_concurrency_level=max_concurrency_level,
            statement_queued_timeout_in_seconds=statement_queued_timeout_in_seconds,
            statement_timeout_in_seconds=statement_timeout_in_seconds,
            tags=tags,
        )
