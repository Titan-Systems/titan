from dataclasses import dataclass

from .__resource import Resource, ResourceSpec
from ..enums import ParseableEnum, ResourceType
from ..scope import AccountScope


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


@dataclass
class _FailoverGroup(ResourceSpec):
    name: str
    object_types: list[ObjectType]
    allowed_accounts: list[str]
    allowed_databases: list[str] = None
    allowed_shares: list[str] = None
    allowed_integration_types: list[IntegrationTypes] = None
    ignore_edition_check: bool = None
    replication_schedule: str = None
    owner: str = "ACCOUNTADMIN"


class FailoverGroup(Resource):
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

    resource_type = ResourceType.FAILOVER_GROUP
    props = Props(
        object_types=EnumListProp("object_types", ObjectType),
        allowed_databases=IdentifierListProp("allowed_databases"),
        allowed_shares=IdentifierListProp("allowed_shares"),
        allowed_integration_types=EnumListProp("allowed_integration_types", IntegrationTypes),
        allowed_accounts=IdentifierListProp("allowed_accounts"),
        ignore_edition_check=FlagProp("ignore edition check"),
        replication_schedule=StringProp("replication_schedule"),
    )
    scope = AccountScope()
    spec = _FailoverGroup

    def __init__(
        self,
        name: str,
        object_types: list[ObjectType],
        allowed_accounts: list[str],
        allowed_databases: list[str] = None,
        allowed_shares: list[str] = None,
        allowed_integration_types: list[IntegrationTypes] = None,
        ignore_edition_check: bool = None,
        replication_schedule: str = None,
        owner: str = "ACCOUNTADMIN",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._data = _FailoverGroup(
            name=name,
            object_types=object_types,
            allowed_accounts=allowed_accounts,
            allowed_databases=allowed_databases,
            allowed_shares=allowed_shares,
            allowed_integration_types=allowed_integration_types,
            ignore_edition_check=ignore_edition_check,
            replication_schedule=replication_schedule,
            owner=owner,
        )
