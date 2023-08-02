from typing import List

from .base import Resource, AccountScoped
from ..enums import ParseableEnum
from ..props import Props, EnumListProp, StringListProp, FlagProp, StringProp, IdentifierListProp


class ObjectType(ParseableEnum):
    ACCOUNT_PARAMETERS = "ACCOUNT PARAMETERS"
    DATABASES = "DATABASES"
    INTEGRATIONS = "INTEGRATIONS"
    NETWORK_POLICIES = "NETWORK POLICIES"
    RESOURCE_MONITORS = "RESOURCE MONITORS"
    ROLES = "ROLES"
    SHARES = "SHARES"
    USERS = "USERS"
    WAREHOUSES = "WAREHOUSES"


class IntegrationTypes(ParseableEnum):
    SECURITY_INTEGRATIONS = "SECURITY INTEGRATIONS"
    API_INTEGRATIONS = "API INTEGRATIONS"
    NOTIFICATION_INTEGRATIONS = "NOTIFICATION INTEGRATIONS"


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
        object_types=EnumListProp("object_types", ObjectType),
        allowed_databases=StringListProp("allowed_databases"),
        allowed_shares=StringListProp("allowed_shares"),
        allowed_integration_types=EnumListProp("allowed_integration_types", IntegrationTypes),
        allowed_accounts=IdentifierListProp("allowed_accounts"),
        ignore_edition_check=FlagProp("ignore edition check"),
        replication_schedule=StringProp("replication_schedule"),
    )

    name: str
    owner: str = None
    object_types: List[ObjectType]
    allowed_databases: List[str] = None
    allowed_shares: List[str] = None
    allowed_integration_types: List[IntegrationTypes] = None
    allowed_accounts: List[str]
    ignore_edition_check: bool = None
    replication_schedule: str = None
