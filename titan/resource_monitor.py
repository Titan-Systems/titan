from typing import Union, Optional, Dict

from .props import (
    EnumProp,
    IntProp,
    ParseableEnum,
    Props,
    StringProp,
    StringListProp,
)


from .resource import Resource, Namespace


class ResourceMonitorFrequency(ParseableEnum):
    MONTHLY = "MONTHLY"
    DAILY = "DAILY"
    WEEKLY = "WEEKLY"
    YEARLY = "YEARLY"
    NEVER = "NEVER"


class ResourceMonitor(Resource):  #
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
    namespace = Namespace.ACCOUNT
    props = Props(
        _start_token="WITH",
        credit_quota=IntProp("CREDIT_QUOTA"),
        frequency=EnumProp("FREQUENCY", ResourceMonitorFrequency),
        start_timestamp=StringProp("START_TIMESTAMP", alt_tokens=["IMMEDIATELY"]),
        end_timestamp=StringProp("END_TIMESTAMP"),
        notify_users=StringListProp("NOTIFY_USERS"),
    )

    name: str
    credit_quota: int = None
    frequency: ResourceMonitorFrequency = None
    start_timestamp: str = None
    end_timestamp: str = None
    notify_users: list = []

    # self.notify_users: List[User] = [u if isinstance(u, User) else User.all[u] for u in notify_users]
    # self.requires(*self.notify_users)

    # @property
    # def sql(self):
    #     props = self.props_sql()
    #     return f"CREATE RESOURCE MONITOR {self.fully_qualified_name} WITH {props}"
