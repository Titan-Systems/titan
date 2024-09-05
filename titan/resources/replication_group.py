from dataclasses import dataclass

from ..enums import AccountEdition, ParseableEnum, ResourceType
from ..props import (
    EnumListProp,
    FlagProp,
    IdentifierListProp,
    Props,
    StringProp,
)
from ..resource_name import ResourceName
from ..scope import AccountScope
from .database import Database
from .resource import NamedResource, Resource, ResourceSpec
from .role import Role
from .share import Share


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


class ReplicationGroup(NamedResource, Resource):
    """
    Description:
        A replication group in Snowflake.

    Snowflake Docs:
        https://docs.snowflake.com/en/sql-reference/sql/create-replication-group

    Fields:
        name (string, required): The name of the replication group.
        object_types (list, required): The object types to be replicated.
        allowed_accounts (list, required): The accounts allowed to replicate.
        allowed_databases (list): The databases allowed to replicate.
        allowed_shares (list): The shares allowed to replicate.
        allowed_integration_types (list): The integration types allowed to replicate.
        ignore_edition_check (bool): Whether to ignore the edition check.
        replication_schedule (string): The replication schedule.
        owner (string or Role): The owner of the replication group. Defaults to "SYSADMIN".

    Python:

        ```python
        replication_group = ReplicationGroup(
            name="some_replication_group",
            object_types=["DATABASES"],
            allowed_accounts=["account1", "account2"],
        )
        ```

    Yaml:

        ```yaml
        replication_groups:
          - name: some_replication_group
            object_types:
              - DATABASES
            allowed_accounts:
              - account1
              - account2
        ```
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
