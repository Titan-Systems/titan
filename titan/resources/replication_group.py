from dataclasses import dataclass

from .resource import Resource, ResourceSpec, ResourceNameTrait
from .database import Database
from .role import Role
from .share import Share
from ..resource_name import ResourceName
from ..enums import AccountEdition, ParseableEnum, ResourceType
from ..props import (
    FlagProp,
    EnumListProp,
    IdentifierListProp,
    Props,
    StringProp,
)
from ..scope import AccountScope


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


class IntegrationType(ParseableEnum):
    SECURITY_INTEGRATIONS = "SECURITY INTEGRATIONS"
    API_INTEGRATIONS = "API INTEGRATIONS"
    STORAGE_INTEGRATIONS = "STORAGE INTEGRATIONS"
    EXTERNAL_ACCESS_INTEGRATIONS = "EXTERNAL ACCESS INTEGRATIONS"
    NOTIFICATION_INTEGRATIONS = "NOTIFICATION INTEGRATIONS"


@dataclass(unsafe_hash=True)
class _ReplicationGroup(ResourceSpec):
    name: ResourceName
    object_types: list[ObjectType]
    allowed_accounts: list[str]
    allowed_databases: list[Database] = None
    allowed_shares: list[Share] = None
    allowed_integration_types: list[IntegrationType] = None
    ignore_edition_check: bool = None
    replication_schedule: str = None
    owner: Role = "SYSADMIN"


class ReplicationGroup(ResourceNameTrait, Resource):
    """
    CREATE REPLICATION GROUP [ IF NOT EXISTS ] <name>
        OBJECT_TYPES = <object_type> [ , <object_type> , ... ]
        [ ALLOWED_DATABASES = <db_name> [ , <db_name> , ... ] ]
        [ ALLOWED_SHARES = <share_name> [ , <share_name> , ... ] ]
        [ ALLOWED_INTEGRATION_TYPES = <integration_type_name> [ , <integration_type_name> , ... ] ]
        ALLOWED_ACCOUNTS = <org_name>.<target_account_name> [ , <org_name>.<target_account_name> , ... ]
        [ IGNORE EDITION CHECK ]
        [ REPLICATION_SCHEDULE = '{ <num> MINUTE | USING CRON <expr> <time_zone> }' ]
    """

    edition = {AccountEdition.ENTERPRISE, AccountEdition.BUSINESS_CRITICAL}
    resource_type = ResourceType.REPLICATION_GROUP
    props = Props(
        object_types=EnumListProp("object_types", ObjectType),
        allowed_databases=IdentifierListProp("allowed_databases"),
        allowed_shares=IdentifierListProp("allowed_shares"),
        allowed_integration_types=EnumListProp("allowed_integration_types", IntegrationType),
        allowed_accounts=IdentifierListProp("allowed_accounts"),
        ignore_edition_check=FlagProp("ignore edition check"),
        replication_schedule=StringProp("replication_schedule"),
    )
    scope = AccountScope()
    spec = _ReplicationGroup

    def __init__(
        self,
        name: str,
        object_types: list[ObjectType],
        allowed_accounts: list[str],
        allowed_databases: list[Database] = None,
        allowed_shares: list[str] = None,
        allowed_integration_types: list[IntegrationType] = None,
        ignore_edition_check: bool = None,
        replication_schedule: str = None,
        owner: str = "SYSADMIN",
        **kwargs,
    ):
        super().__init__(name, **kwargs)
        self._data: _ReplicationGroup = _ReplicationGroup(
            name=self._name,
            object_types=object_types,
            allowed_accounts=allowed_accounts,
            allowed_databases=allowed_databases,
            allowed_shares=allowed_shares,
            allowed_integration_types=allowed_integration_types,
            ignore_edition_check=ignore_edition_check,
            replication_schedule=replication_schedule,
            owner=owner,
        )
        self.requires(self._data.allowed_databases or [])
