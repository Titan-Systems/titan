import re

from typing import Union, Optional, List, Tuple

from .resource import AccountLevelResource
from .resource_monitor import ResourceMonitor
from .parseable_enum import ParseableEnum
from .props import Identifier, BoolProp, EnumProp, StringProp, IntProp, TagsProp, IdentifierProp


class WarehouseType(ParseableEnum):
    STANDARD = "STANDARD"
    SNOWPARK_OPTIMIZED = "SNOWPARK-OPTIMIZED"


class WarehouseSize(ParseableEnum):
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


class Warehouse(AccountLevelResource):  #
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

    resource_name = "WAREHOUSE"

    props = {
        "warehouse_type": EnumProp("WAREHOUSE_TYPE", WarehouseType),
        "warehouse_size": EnumProp("WAREHOUSE_SIZE", WarehouseSize),
        "max_cluster_count": IntProp("MAX_CLUSTER_COUNT"),
        "min_cluster_count": IntProp("MIN_CLUSTER_COUNT"),
        "scaling_policy": EnumProp("SCALING_POLICY", WarehouseScalingPolicy),
        "auto_suspend": IntProp("AUTO_SUSPEND"),  # TODO: find some way to make this nullable
        "auto_resume": BoolProp("AUTO_RESUME"),
        "initially_suspended": BoolProp("INITIALLY_SUSPENDED"),
        "resource_monitor": IdentifierProp("RESOURCE_MONITOR"),
        "comment": StringProp("COMMENT"),
        "enable_query_acceleration": BoolProp("ENABLE_QUERY_ACCELERATION"),
        "query_acceleration_max_scale_factor": IntProp("QUERY_ACCELERATION_MAX_SCALE_FACTOR"),
        "max_concurrency_level": IntProp("MAX_CONCURRENCY_LEVEL"),
        "statement_queued_timeout_in_seconds": IntProp("STATEMENT_QUEUED_TIMEOUT_IN_SECONDS"),
        "statement_timeout_in_seconds": IntProp("STATEMENT_TIMEOUT_IN_SECONDS"),
        "tags": TagsProp(),
    }

    create_statement = re.compile(
        rf"""
            CREATE\s+
            (?:OR\s+REPLACE\s+)?
            WAREHOUSE\s+
            (?:IF\s+NOT\s+EXISTS\s+)?
            ({Identifier.pattern})\s+
            (?:WITH)?
            """,
        re.IGNORECASE | re.VERBOSE,
    )

    ownable = True

    def __init__(
        self,
        warehouse_type: Union[None, str, WarehouseType] = None,
        warehouse_size: Union[None, str, WarehouseSize] = None,
        max_cluster_count: Optional[int] = None,
        min_cluster_count: Optional[int] = None,
        scaling_policy: Optional[str] = None,
        auto_suspend: Optional[int] = None,
        auto_resume: Optional[bool] = None,
        initially_suspended: Optional[bool] = None,
        resource_monitor: Union[None, str, ResourceMonitor] = None,
        comment: Optional[str] = None,
        enable_query_acceleration: Optional[bool] = None,
        query_acceleration_max_scale_factor: Optional[int] = None,
        max_concurrency_level: Optional[int] = None,
        statement_queued_timeout_in_seconds: Optional[int] = None,
        statement_timeout_in_seconds: Optional[int] = None,
        tags: List[Tuple[str, str]] = [],
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.warehouse_type = WarehouseType.parse(warehouse_type) if warehouse_type else WarehouseType.STANDARD
        self.warehouse_size = WarehouseSize.parse(warehouse_size) if warehouse_size else WarehouseSize.XSMALL
        self.max_cluster_count = max_cluster_count
        self.min_cluster_count = min_cluster_count
        self.scaling_policy = scaling_policy
        self.auto_suspend = auto_suspend
        self.auto_resume = auto_resume
        self.initially_suspended = initially_suspended
        self.resource_monitor = (
            ResourceMonitor.all[resource_monitor] if isinstance(resource_monitor, str) else resource_monitor
        )
        self.comment = comment
        self.enable_query_acceleration = enable_query_acceleration
        self.query_acceleration_max_scale_factor = query_acceleration_max_scale_factor
        self.max_concurrency_level = max_concurrency_level
        self.statement_queued_timeout_in_seconds = statement_queued_timeout_in_seconds
        self.statement_timeout_in_seconds = statement_timeout_in_seconds
        self.tags = tags
        self.requires(self.resource_monitor)
