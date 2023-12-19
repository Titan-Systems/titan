from typing import List
from typing_extensions import Annotated

from pydantic import BeforeValidator

from .base import AccountScoped, Resource, _fix_class_documentation, coerce_from_str, serialize_resource_by_name
from ..enums import ParseableEnum
from ..props import (
    EnumProp,
    IntProp,
    Props,
    StringProp,
    StringListProp,
)


class ResourceMonitorFrequency(ParseableEnum):
    MONTHLY = "MONTHLY"
    DAILY = "DAILY"
    WEEKLY = "WEEKLY"
    YEARLY = "YEARLY"
    NEVER = "NEVER"


@_fix_class_documentation
class ResourceMonitor(AccountScoped, Resource):
    """
    CREATE [ OR REPLACE ] RESOURCE MONITOR <name> WITH
                          [ CREDIT_QUOTA = <number> ]
                          [ FREQUENCY = { MONTHLY | DAILY | WEEKLY | YEARLY | NEVER } ]
                          [ START_TIMESTAMP = { <timestamp> | IMMEDIATELY } ]
                          [ END_TIMESTAMP = <timestamp> ]
                          [ NOTIFY_USERS = ( <user_name> [ , <user_name> , ... ] ) ]
                          [ TRIGGERS triggerDefinition [ triggerDefinition ... ] ]

    triggerDefinition ::=
        ON <threshold> PERCENT DO { SUSPEND | SUSPEND_IMMEDIATE | NOTIFY }
    """

    resource_type = "RESOURCE MONITOR"
    props = Props(
        _start_token="WITH",
        credit_quota=IntProp("credit_quota"),
        frequency=EnumProp("frequency", ResourceMonitorFrequency),
        start_timestamp=StringProp("start_timestamp", alt_tokens=["IMMEDIATELY"]),
        end_timestamp=StringProp("end_timestamp"),
        notify_users=StringListProp("notify_users", parens=True),
    )

    name: str
    credit_quota: int = None
    frequency: ResourceMonitorFrequency = None
    start_timestamp: str = None
    end_timestamp: str = None
    notify_users: List[str] = None


T_ResourceMonitor = Annotated[
    ResourceMonitor,
    BeforeValidator(coerce_from_str(ResourceMonitor)),
    serialize_resource_by_name,
]
