from typing import List

from ..resource import Resource, AccountScoped
from ..props import Props, StringListProp, FlagProp, StringProp


class FailoverGroup(Resource, AccountScoped):
    """
    CREATE FAILOVER GROUP [ IF NOT EXISTS ] <name>
        OBJECT_TYPES = <object_type> [ , <object_type> , ... ]
        [ ALLOWED_DATABASES = <db_name> [ , <db_name> , ... ] ]
        [ ALLOWED_SHARES = <share_name> [ , <share_name> , ... ] ]
        [ ALLOWED_INTEGRATION_TYPES = <integration_type_name> [ , <integration_type_name> , ... ] ]
        ALLOWED_ACCOUNTS = <org_name>.<target_account_name> [ , <org_name>.<target_account_name> ,  ... ]
        [ IGNORE EDITION CHECK ]
        [ REPLICATION_SCHEDULE = '{ <num> MINUTE | USING CRON <expr> <time_zone> }' ]
    """

    resource_type = "FAILOVER GROUP"
    props = Props(
        object_types=StringListProp("object_types"),
        allowed_databases=StringListProp("allowed_databases"),
        allowed_shares=StringListProp("allowed_shares"),
        allowed_integration_types=StringListProp("allowed_integration_types"),
        allowed_accounts=StringListProp("allowed_accounts"),
        ignore_edition_check=FlagProp("ignore edition check"),
        replication_schedule=StringProp("replication_schedule"),
    )

    name: str
    owner: str = None
    object_types: List[str]
    allowed_databases: List[str] = None
    allowed_shares: List[str] = None
    allowed_integration_types: List[str] = None
    allowed_accounts: List[str]
    ignore_edition_check: bool = None
    replication_schedule: str = None
