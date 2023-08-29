from typing import Dict
from typing_extensions import Annotated

from pydantic import BeforeValidator

from .base import Resource, AccountScoped, _fix_class_documentation, serialize_resource_by_name, coerce_from_str
from .resource_monitor import T_ResourceMonitor
from ..enums import ParseableEnum
from ..privs import GlobalPriv, Privs, WarehousePriv
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


@_fix_class_documentation
class Warehouse(Resource, AccountScoped):
    """
    CREATE [ OR REPLACE ] WAREHOUSE [ IF NOT EXISTS ] <name>
        [ [ WITH ] objectProperties ]
        [ objectParams ]

    objectProperties ::=
        WAREHOUSE_TYPE = STANDARD | SNOWPARK-OPTIMIZED
        WAREHOUSE_SIZE = XSMALL | SMALL | MEDIUM | LARGE | XLARGE | XXLARGE | XXXLARGE
                          | X4LARGE | X5LARGE | X6LARGE
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

    resource_type = "WAREHOUSE"
    lifecycle_privs = Privs(
        create=GlobalPriv.CREATE_WAREHOUSE,
        delete=WarehousePriv.OWNERSHIP,
    )
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
    resource_monitor: T_ResourceMonitor = None
    comment: str = None
    enable_query_acceleration: bool = None
    query_acceleration_max_scale_factor: int = None
    max_concurrency_level: int = 8
    statement_queued_timeout_in_seconds: int = 0
    statement_timeout_in_seconds: int = 172800
    tags: Dict[str, str] = None


T_Warehouse = Annotated[
    Warehouse,
    BeforeValidator(coerce_from_str(Warehouse)),
    serialize_resource_by_name,
]
