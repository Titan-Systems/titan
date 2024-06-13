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


@dataclass(unsafe_hash=True)
class _ResourceMonitor(ResourceSpec):
    name: str
    credit_quota: int = None
    frequency: ResourceMonitorFrequency = None
    start_timestamp: str = None
    end_timestamp: str = None
    notify_users: list[str] = None


class ResourceMonitor(Resource):
    """
    Description:
        Manages the monitoring of resource usage within an account.

    Snowflake Docs:
        https://docs.snowflake.com/en/sql-reference/sql/create-resource-monitor

    Fields:
        name (string, required): The name of the resource monitor.
        credit_quota (int): The amount of credits that can be used by this monitor. Defaults to None.
        frequency (string or ResourceMonitorFrequency): The frequency of monitoring. Defaults to None.
        start_timestamp (string): The start time for the monitoring period. Defaults to None.
        end_timestamp (string): The end time for the monitoring period. Defaults to None.
        notify_users (list): A list of users to notify when thresholds are reached. Defaults to None.

    Python:

        resource_monitor = ResourceMonitor(
            name="some_resource_monitor",
            credit_quota=1000,
            frequency="DAILY",
            start_timestamp="2022-01-01T00:00:00Z",
            end_timestamp="2022-12-31T23:59:59Z",
            notify_users=["user1", "user2"]
        )

    Yaml:

        resource_monitor:
          - name: some_resource_monitor
            credit_quota: 1000
            frequency: DAILY
            start_timestamp: 2022-01-01T00:00:00Z
            end_timestamp: 2022-12-31T23:59:59Z
            notify_users:
              - user1
              - user2
    """

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
        frequency: ResourceMonitorFrequency = ResourceMonitorFrequency.MONTHLY,
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
