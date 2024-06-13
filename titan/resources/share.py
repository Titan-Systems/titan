from dataclasses import dataclass

from .resource import Resource, ResourceSpec
from .role import Role
from ..enums import ResourceType
from ..resource_name import ResourceName
from ..scope import AccountScope

from ..props import (
    Props,
    StringProp,
)


@dataclass(unsafe_hash=True)
class _Share(ResourceSpec):
    name: ResourceName
    owner: Role = "SYSADMIN"
    comment: str = None


class Share(Resource):
    """
    Description:
        Represents a share resource in Snowflake, which allows sharing data across Snowflake accounts.

    Snowflake Docs:
        https://docs.snowflake.com/en/sql-reference/sql/create-share

    Fields:
        name (string, required): The name of the share.
        owner (string or Role): The owner of the share. Defaults to "SYSADMIN".
        comment (string): A comment about the share.

    Python:

        share = Share(
            name="some_share",
            comment="This is a snowflake share."
        )

    Yaml:

        share:
          - name: some_share
            comment: This is a snowflake share.
    """

    resource_type = ResourceType.SHARE
    props = Props(
        comment=StringProp("comment"),
    )
    scope = AccountScope()
    spec = _Share

    def __init__(
        self,
        name: str,
        owner: str = "SYSADMIN",
        comment: str = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._data = _Share(
            name=name,
            owner=owner,
            comment=comment,
        )

    @property
    def name(self):
        return self._data.name
