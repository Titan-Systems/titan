import re

from enum import Enum
from typing import Union, Optional, List

from sqlglot import exp

from .resource import AccountLevelResource
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


class Warehouse(AccountLevelResource):
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

    props = {
        "warehouse_type": WarehouseType,
        "warehouse_size": WarehouseSize,
        "max_cluster_count": int,
        "min_cluster_count": int,
    }

    def __init__(
        self,
        warehouse_type: Optional[WAREHOUSE_TYPE_T] = WarehouseType.STANDARD,
        warehouse_size: Optional[WAREHOUSE_SIZE_T] = WarehouseSize.XSMALL,
        max_cluster_count: Optional[int] = None,
        min_cluster_count: Optional[int] = None,
        scaling_policy: Optional[str] = None,
        auto_suspend: Optional[int] = None,
        auto_resume: Optional[bool] = None,
        initially_suspended: Optional[bool] = None,
        # resource_monitor=None,
        comment: Optional[str] = None,
        enable_query_acceleration: Optional[bool] = None,
        query_acceleration_max_scale_factor: Optional[int] = None,
        max_concurrency_level: Optional[int] = None,
        statement_queued_timeout_in_seconds: Optional[int] = None,
        statement_timeout_in_seconds: Optional[int] = None,
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

    @classmethod
    def parse_props(cls, sql: str):
        found_props = {}

        for prop_name, prop_type in cls.props.items():
            pattern = rf"{prop_name}\s*=\s*'?(\w+)'?"
            match = re.search(pattern, sql, re.IGNORECASE)
            if match:
                prop_value = match.group(1)
                if issubclass(prop_type, ParsableEnum):
                    prop_value = prop_type.parse(prop_value)
                elif prop_type == int:
                    prop_value = int(prop_value)
                found_props[prop_name] = prop_value

        return found_props

    @classmethod
    def from_expression(cls, expression: exp.Command):
        """
        (COMMAND
            this: CREATE,
            expression:  WAREHOUSE DATA_APPS_ADHOC WITH WAREHOUSE_SIZE='small')
        """

        sql = expression.args["expression"]

        identifier = re.compile(r"WAREHOUSE\s+([A-Z][A-Z0-9_]*)", re.IGNORECASE)
        match = re.search(identifier, sql)

        if match is None:
            raise Exception
        name = match.group(1)
        props = cls.parse_props(sql[match.end() :])

        return cls(name=name, **props)


# WAREHOUSE_T = Union[str, Warehouse]


# def parse_warehouse(value: WAREHOUSE_T) -> Warehouse:
#     if isinstance(value, str):
