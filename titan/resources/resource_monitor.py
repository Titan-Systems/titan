from dataclasses import dataclass

from .resource import Resource, ResourceSpec
from ..enums import ParseableEnum, ResourceType
from ..scope import AccountScope
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


@dataclass
class _ResourceMonitor(ResourceSpec):
    name: str
    credit_quota: int = None
    frequency: ResourceMonitorFrequency = None
    start_timestamp: str = None
    end_timestamp: str = None
    notify_users: list[str] = None


class ResourceMonitor(Resource):
    resource_type = ResourceType.RESOURCE_MONITOR
    props = Props(
        _start_token="WITH",
        credit_quota=IntProp("credit_quota"),
        frequency=EnumProp("frequency", ResourceMonitorFrequency),
        start_timestamp=StringProp("start_timestamp", alt_tokens=["IMMEDIATELY"]),
        end_timestamp=StringProp("end_timestamp"),
        notify_users=StringListProp("notify_users", parens=True),
    )
    scope = AccountScope()
    spec = _ResourceMonitor

    def __init__(
        self,
        name: str,
        credit_quota: int = None,
        frequency: ResourceMonitorFrequency = None,
        start_timestamp: str = None,
        end_timestamp: str = None,
        notify_users: list[str] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._data = _ResourceMonitor(
            name=name,
            credit_quota=credit_quota,
            frequency=frequency,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
            notify_users=notify_users,
        )
