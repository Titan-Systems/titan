from enum import Enum

from typing import Union, Optional

from .entity import AccountLevelEntity
from .helpers import ParsableEnum


class WarehouseType(ParsableEnum):
    STANDARD = "STANDARD"
    SNOWPARK_OPTIMIZED = "SNOWPARK-OPTIMIZED"


WAREHOUSE_TYPE_T = Union[str, WarehouseType]


class WarehouseSize(ParsableEnum):
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


WAREHOUSE_SIZE_T = Union[str, WarehouseSize]


class Warehouse(AccountLevelEntity):
    """
    CREATE [ OR REPLACE ] WAREHOUSE [ IF NOT EXISTS ] <name>
            [ [ WITH ] objectProperties ]
            [ objectParams ]

    objectProperties ::=
      WAREHOUSE_TYPE = STANDARD | SNOWPARK-OPTIMIZED
      WAREHOUSE_SIZE = XSMALL | SMALL | MEDIUM | LARGE | XLARGE | XXLARGE | XXXLARGE | X4LARGE | X5LARGE | X6LARGE
      MAX_CLUSTER_COUNT = <num>
      MIN_CLUSTER_COUNT = <num>
      SCALING_POLICY = STANDARD | ECONOMY
      AUTO_SUSPEND = <num> | NULL
      AUTO_RESUME = TRUE | FALSE
      INITIALLY_SUSPENDED = TRUE | FALSE
      RESOURCE_MONITOR = <monitor_name>
      COMMENT = '<string_literal>'
      ENABLE_QUERY_ACCELERATION = TRUE | FALSE
      QUERY_ACCELERATION_MAX_SCALE_FACTOR = <num>

    objectParams ::=
      MAX_CONCURRENCY_LEVEL = <num>
      STATEMENT_QUEUED_TIMEOUT_IN_SECONDS = <num>
      STATEMENT_TIMEOUT_IN_SECONDS = <num>
      [ [ WITH ] TAG ( <tag_name> = '<tag_value>' [ , <tag_name> = '<tag_value>' , ... ] ) ]
    """

    def __init__(
        self,
        warehouse_type: Optional[WAREHOUSE_TYPE_T] = WarehouseType.STANDARD,
        warehouse_size: Optional[WAREHOUSE_SIZE_T] = WarehouseSize.XSMALL,
        max_cluster_count: int = None,
        min_cluster_count: int = None,
        scaling_policy: str = None,
        auto_suspend: int = None,
        auto_resume: bool = None,
        initially_suspended: bool = None,
        # resource_monitor=None,
        comment: str = None,
        enable_query_acceleration: bool = None,
        query_acceleration_max_scale_factor: int = None,
        max_concurrency_level: int = None,
        statement_queued_timeout_in_seconds: int = None,
        statement_timeout_in_seconds: int = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.warehouse_type = WarehouseType.parse(warehouse_type)
        self.warehouse_size = WarehouseSize.parse(warehouse_size)
        self.max_cluster_count = max_cluster_count
        self.min_cluster_count = min_cluster_count
        self.scaling_policy = scaling_policy
        self.auto_suspend = auto_suspend
        self.auto_resume = auto_resume
        self.initially_suspended = initially_suspended
        # self.resource_monitor = resource_monitor
        self.comment = comment
        self.enable_query_acceleration = enable_query_acceleration
        self.query_acceleration_max_scale_factor = query_acceleration_max_scale_factor
        self.max_concurrency_level = max_concurrency_level
        self.statement_queued_timeout_in_seconds = statement_queued_timeout_in_seconds
        self.statement_timeout_in_seconds = statement_timeout_in_seconds


# WAREHOUSE_T = Union[str, Warehouse]


# def parse_warehouse(value: WAREHOUSE_T) -> Warehouse:
#     if isinstance(value, str):
