from dataclasses import dataclass

from .resource import Resource, ResourceContainer, ResourceSpec
from ..enums import AccountEdition, ResourceType
from ..props import Props
from ..scope import OrganizationScope


@dataclass(unsafe_hash=True)
class _Account(ResourceSpec):
    name: str
    locator: str
    edition: AccountEdition = None
    region: str = None
    comment: str = None


class Account(Resource, ResourceContainer):
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
        super().__init__(**kwargs)
        self._data: _Account = _Account(
            name=name,
            locator=locator,
            edition=edition,
            comment=comment,
        )
