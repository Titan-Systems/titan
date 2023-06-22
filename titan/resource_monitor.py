import re

from typing import Union, Optional, List

from .resource import AccountLevelResource
from .user import User
from .props import Identifier, EnumProp, ParsableEnum, StringProp, IntProp, IdentifierListProp


class ResourceMonitorFrequency(ParsableEnum):
    MONTHLY = "MONTHLY"
    DAILY = "DAILY"
    WEEKLY = "WEEKLY"
    YEARLY = "YEARLY"
    NEVER = "NEVER"


class ResourceMonitor(AccountLevelResource):  #
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

    props = {
        "CREDIT_QUOTA": IntProp("CREDIT_QUOTA"),
        "FREQUENCY": EnumProp("FREQUENCY", ResourceMonitorFrequency),
        "START_TIMESTAMP": StringProp("START_TIMESTAMP"),
        "END_TIMESTAMP": StringProp("END_TIMESTAMP"),
        "NOTIFY_USERS": IdentifierListProp("NOTIFY_USERS"),
        # "TRIGGERS": TagsProp(),
    }

    create_statement = re.compile(
        rf"""
            CREATE\s+
            (?:OR\s+REPLACE\s+)?
            RESOURCE\s+MONITOR\s+
            ({Identifier.pattern})\s+
            WITH""",
        re.IGNORECASE | re.VERBOSE,
    )

    ownable = False

    def __init__(
        self,
        credit_quota: Optional[int] = None,
        frequency: Union[None, str, ResourceMonitorFrequency] = None,
        start_timestamp: Optional[str] = None,
        end_timestamp: Optional[str] = None,
        notify_users: List[Union[str, User]] = [],
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.credit_quota = credit_quota
        self.frequency = frequency
        self.start_timestamp = start_timestamp
        self.end_timestamp = end_timestamp
        self.notify_users: List[User] = [u if isinstance(u, User) else User.all[u] for u in notify_users]
        self.requires(*self.notify_users)

    @property
    def sql(self):
        return f"""
            CREATE RESOURCE MONITOR {self.fully_qualified_name}
                WITH
                {self.props["CREDIT_QUOTA"].render(self.credit_quota)}
                {self.props["FREQUENCY"].render(self.frequency)}
                {self.props["START_TIMESTAMP"].render(self.start_timestamp)}
                {self.props["END_TIMESTAMP"].render(self.end_timestamp)}
                {self.props["NOTIFY_USERS"].render(self.notify_users)}
        """.strip()
