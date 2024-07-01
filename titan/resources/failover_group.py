from dataclasses import dataclass

from .resource import Resource, ResourceSpec, NamedResource
from .role import Role
from ..enums import ParseableEnum, ResourceType
from ..scope import AccountScope
from ..resource_name import ResourceName
from ..props import (
    EnumListProp,
    FlagProp,
    IdentifierListProp,
    Props,
    StringProp,
)


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


@dataclass(unsafe_hash=True)
class _FailoverGroup(ResourceSpec):
    name: ResourceName
    object_types: list[ObjectType]
    allowed_accounts: list[str]
    allowed_databases: list[str] = None
    allowed_shares: list[str] = None
    allowed_integration_types: list[IntegrationTypes] = None
    ignore_edition_check: bool = None
    replication_schedule: str = None
    owner: Role = "ACCOUNTADMIN"


class FailoverGroup(NamedResource, Resource):
    """
    Description:
        Represents a failover group in Snowflake, which is a collection of databases, shares, and other resources
        that can be failed over together to a secondary account in case of a disaster recovery scenario.

    Snowflake Docs:
        https://docs.snowflake.com/en/sql-reference/sql/create-failover-group

    Fields:
        name (string, required): The name of the failover group.
        object_types (list): The types of objects included in the failover group. Can include string or ObjectType.
        allowed_accounts (list, required): The accounts that are allowed to be part of the failover group.
        allowed_databases (list): The databases that are allowed to be part of the failover group.
        allowed_shares (list): The shares that are allowed to be part of the failover group.
        allowed_integration_types (list): The integration types that are allowed in the failover group. Can include string or IntegrationTypes.
        ignore_edition_check (bool): Specifies whether to ignore the edition check. Defaults to None.
        replication_schedule (string): The schedule for replication. Defaults to None.
        owner (string or Role): The owner role of the failover group. Defaults to "ACCOUNTADMIN".

    Python:

        ```python
        failover_group = FailoverGroup(
            name="some_failover_group",
            object_types=["DATABASES", "ROLES"],
            allowed_accounts=["org1.account1", "org2.account2"],
            allowed_databases=["db1", "db2"],
            allowed_shares=["share1", "share2"],
            allowed_integration_types=["SECURITY INTEGRATIONS", "API INTEGRATIONS"],
            ignore_edition_check=True,
            replication_schedule="USING CRON 0 0 * * * UTC",
            owner="ACCOUNTADMIN"
        )
        ```

    Yaml:

        ```yaml
        failover_groups:
          - name: some_failover_group
            object_types:
              - DATABASES
              - ROLES
            allowed_accounts:
              - org1.account1
              - org2.account2
            allowed_databases:
              - db1
              - db2
            allowed_shares:
              - share1
              - share2
            allowed_integration_types:
              - SECURITY INTEGRATIONS
              - API INTEGRATIONS
            ignore_edition_check: true
            replication_schedule: "USING CRON 0 0 * * * UTC"
            owner: ACCOUNTADMIN
        ```
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
        super().__init__(name, **kwargs)
        self._data: _FailoverGroup = _FailoverGroup(
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
