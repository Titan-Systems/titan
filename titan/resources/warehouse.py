from dataclasses import dataclass

from ..enums import ParseableEnum, ResourceType, WarehouseSize
from ..props import (
    BoolProp,
    EnumProp,
    IdentifierProp,
    IntProp,
    Props,
    StringProp,
    TagsProp,
)
from ..resource_name import ResourceName
from ..scope import AccountScope
from .resource import NamedResource, Resource, ResourceSpec
from .resource_monitor import ResourceMonitor
from .role import Role
from .tag import TaggableResource


class WarehouseType(ParseableEnum):
    STANDARD = "STANDARD"
    SNOWPARK_OPTIMIZED = "SNOWPARK-OPTIMIZED"


class WarehouseScalingPolicy(ParseableEnum):
    STANDARD = "STANDARD"
    ECONOMY = "ECONOMY"


@dataclass(unsafe_hash=True)
class _Warehouse(ResourceSpec):
    name: ResourceName
    owner: Role = "SYSADMIN"
    warehouse_type: WarehouseType = WarehouseType.STANDARD
    warehouse_size: WarehouseSize = WarehouseSize.XSMALL
    max_cluster_count: int = None
    min_cluster_count: int = None
    scaling_policy: WarehouseScalingPolicy = None
    auto_suspend: int = 600
    auto_resume: bool = True
    initially_suspended: bool = None
    resource_monitor: ResourceMonitor = None
    comment: str = None
    enable_query_acceleration: bool = False
    query_acceleration_max_scale_factor: int = None
    max_concurrency_level: int = 8
    statement_queued_timeout_in_seconds: int = 0
    statement_timeout_in_seconds: int = 172800


class Warehouse(NamedResource, TaggableResource, Resource):
    """
    Description:
        A virtual warehouse, often referred to simply as a "warehouse", is a cluster of compute resources in Snowflake. It provides the necessary CPU, memory, and temporary storage to execute SQL SELECT statements, perform DML operations such as INSERT, UPDATE, DELETE, and manage data loading and unloading.

    Snowflake Docs:
        https://docs.snowflake.com/en/sql-reference/sql/create-warehouse

    Fields:
        name (string, required): The name of the warehouse.
        owner (string): The owner of the warehouse. Defaults to "SYSADMIN".
        warehouse_type (string or WarehouseType): The type of the warehouse, either STANDARD or SNOWPARK-OPTIMIZED. Defaults to STANDARD.
        warehouse_size (string or WarehouseSize): The size of the warehouse which defines the compute and storage capacity.
        max_cluster_count (int): The maximum number of clusters for the warehouse.
        min_cluster_count (int): The minimum number of clusters for the warehouse.
        scaling_policy (string or WarehouseScalingPolicy): The policy that defines how the warehouse scales.
        auto_suspend (int): The time in seconds of inactivity after which the warehouse is automatically suspended.
        auto_resume (bool): Whether the warehouse should automatically resume when queries are submitted.
        initially_suspended (bool): Whether the warehouse should start in a suspended state.
        resource_monitor (string or ResourceMonitor): The resource monitor that tracks the warehouse's credit usage and other metrics.
        comment (string): A comment about the warehouse.
        enable_query_acceleration (bool): Whether query acceleration is enabled to improve performance.
        query_acceleration_max_scale_factor (int): The maximum scale factor for query acceleration.
        max_concurrency_level (int): The maximum number of concurrent queries that the warehouse can handle.
        statement_queued_timeout_in_seconds (int): The time in seconds a statement can be queued before it times out.
        statement_timeout_in_seconds (int): The time in seconds a statement can run before it times out.
        tags (dict): Tags for the warehouse.

    Python:

        ```python
        warehouse = Warehouse(
            name="some_warehouse",
            owner="SYSADMIN",
            warehouse_type="STANDARD",
            warehouse_size="XSMALL",
            max_cluster_count=10,
            min_cluster_count=1,
            scaling_policy="STANDARD",
            auto_suspend=600,
            auto_resume=True,
            initially_suspended=False,
            resource_monitor=None,
            comment="This is a test warehouse",
            enable_query_acceleration=False,
            query_acceleration_max_scale_factor=1,
            max_concurrency_level=8,
            statement_queued_timeout_in_seconds=0,
            statement_timeout_in_seconds=172800,
            tags={"env": "test"},
        )
        ```

    Yaml:

        ```yaml
        warehouses:
          - name: some_warehouse
            owner: SYSADMIN
            warehouse_type: STANDARD
            warehouse_size: XSMALL
            max_cluster_count: 10
            min_cluster_count: 1
            scaling_policy: STANDARD
            auto_suspend: 600
            auto_resume: true
            initially_suspended: false
            resource_monitor: null
            comment: This is a test warehouse
            enable_query_acceleration: false
            query_acceleration_max_scale_factor: 1
            max_concurrency_level: 8
            statement_queued_timeout_in_seconds: 0
            statement_timeout_in_seconds: 172800
            tags:
              env: test
        ```
    """

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
        warehouse_size: WarehouseSize = WarehouseSize.XSMALL,
        max_cluster_count: int = None,
        min_cluster_count: int = None,
        scaling_policy: WarehouseScalingPolicy = None,
        auto_suspend: int = 600,
        auto_resume: bool = True,
        initially_suspended: bool = None,
        resource_monitor: ResourceMonitor = None,
        comment: str = None,
        enable_query_acceleration: bool = False,
        query_acceleration_max_scale_factor: int = None,
        max_concurrency_level: int = 8,
        statement_queued_timeout_in_seconds: int = 0,
        statement_timeout_in_seconds: int = 172800,
        tags: dict[str, str] = None,
        **kwargs,
    ):
        super().__init__(name, **kwargs)
        self._data: _Warehouse = _Warehouse(
            name=self._name,
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
        )
        self.set_tags(tags)
