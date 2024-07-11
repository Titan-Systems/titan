from dataclasses import dataclass

from ..enums import AccountEdition, ResourceType
from ..props import Props
from ..resource_name import ResourceName
from ..scope import OrganizationScope
from .resource import NamedResource, Resource, ResourceContainer, ResourceSpec


@dataclass(unsafe_hash=True)
class _Account(ResourceSpec):
    name: ResourceName
    locator: str
    edition: AccountEdition = None
    region: str = None
    comment: str = None


class Account(NamedResource, Resource, ResourceContainer):
    """
    CREATE ACCOUNT <name>
        ADMIN_NAME = <string>
        { ADMIN_PASSWORD = '<string_literal>' | ADMIN_RSA_PUBLIC_KEY = <string> }
        [ FIRST_NAME = <string> ]
        [ LAST_NAME = <string> ]
        EMAIL = '<string>'
        [ MUST_CHANGE_PASSWORD = { TRUE | FALSE } ]
        EDITION = { STANDARD | ENTERPRISE | BUSINESS_CRITICAL }
        [ REGION_GROUP = <region_group_id> ]
        [ REGION = <snowflake_region_id> ]
        [ COMMENT = '<string_literal>' ]
    """

    resource_type = ResourceType.ACCOUNT
    props = Props()
    scope = OrganizationScope()
    spec = _Account

    def __init__(
        self,
        name: str,
        locator: str,
        edition: AccountEdition = None,
        comment: str = None,
        **kwargs,
    ):
        super().__init__(name=name, **kwargs)
        self._data: _Account = _Account(
            name=self._name,
            locator=locator,
            edition=edition,
            comment=comment,
        )
