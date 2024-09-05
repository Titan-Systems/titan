from dataclasses import dataclass

from ..enums import ParseableEnum, ResourceType
from ..props import (
    EnumProp,
    IntProp,
    Props,
    StringListProp,
    StringProp,
)
from ..resource_name import ResourceName
from ..scope import AccountScope
from .resource import NamedResource, Resource, ResourceSpec
from .role import Role


class ResourceMonitorFrequency(ParseableEnum):
    MONTHLY = "MONTHLY"
    DAILY = "DAILY"
    WEEKLY = "WEEKLY"
    YEARLY = "YEARLY"
    NEVER = "NEVER"


@dataclass(unsafe_hash=True)
class _ResourceMonitor(ResourceSpec):
    name: ResourceName
    owner: Role = "ACCOUNTADMIN"
    credit_quota: int = None
    frequency: ResourceMonitorFrequency = None
    start_timestamp: str = None
    end_timestamp: str = None
    notify_users: list[str] = None

    def __post_init__(self):
        super().__post_init__()
        if self.credit_quota is not None and not isinstance(self.credit_quota, int):
            raise ValueError("credit_quota must be an integer or None")
        if self.start_timestamp and self.frequency is None:
            self.frequency = ResourceMonitorFrequency.MONTHLY
        if self.owner.name != "ACCOUNTADMIN":
            raise ValueError("ResourceMonitors can only be created by ACCOUNTADMIN")


class ResourceMonitor(NamedResource, Resource):
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

        ```python
        resource_monitor = ResourceMonitor(
            name="some_resource_monitor",
            credit_quota=1000,
            frequency="DAILY",
            start_timestamp="2049-01-01 00:00",
            end_timestamp="2049-12-31 23:59",
            notify_users=["user1", "user2"]
        )
        ```

    Yaml:

        ```yaml
        resource_monitors:
          - name: some_resource_monitor
            credit_quota: 1000
            frequency: DAILY
            start_timestamp: "2049-01-01 00:00"
            end_timestamp: "2049-12-31 23:59"
            notify_users:
              - user1
              - user2
        ```
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
        frequency: ResourceMonitorFrequency = None,
        start_timestamp: str = None,
        end_timestamp: str = None,
        notify_users: list[str] = None,
        owner: str = "ACCOUNTADMIN",
        **kwargs,
    ):
        super().__init__(name, **kwargs)
        self._data: _ResourceMonitor = _ResourceMonitor(
            name=self._name,
            credit_quota=credit_quota,
            frequency=frequency,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
            notify_users=notify_users,
            owner=owner,
        )
        # TODO: rely on notify_users
