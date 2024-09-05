from dataclasses import dataclass

from ..enums import ResourceType
from ..props import (
    Props,
    StringProp,
)
from ..resource_name import ResourceName
from ..scope import AccountScope
from .resource import NamedResource, Resource, ResourceSpec
from .role import Role


@dataclass(unsafe_hash=True)
class _Share(ResourceSpec):
    name: ResourceName
    owner: Role = "ACCOUNTADMIN"
    comment: str = None


class Share(NamedResource, Resource):
    """
    Description:
        Represents a share resource in Snowflake, which allows sharing data across Snowflake accounts.

    Snowflake Docs:
        https://docs.snowflake.com/en/sql-reference/sql/create-share

    Fields:
        name (string, required): The name of the share.
        owner (string or Role): The owner of the share. Defaults to "ACCOUNTADMIN".
        comment (string): A comment about the share.

    Python:

        ```python
        share = Share(
            name="some_share",
            comment="This is a snowflake share."
        )
        ```

    Yaml:

        ```yaml
        shares:
          - name: some_share
            comment: This is a snowflake share.
        ```
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
        owner: str = "ACCOUNTADMIN",
        comment: str = None,
        **kwargs,
    ):
        super().__init__(name, **kwargs)
        self._data = _Share(
            name=self._name,
            owner=owner,
            comment=comment,
        )
