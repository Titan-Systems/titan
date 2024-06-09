from dataclasses import dataclass

from .resource import Resource, ResourceSpec
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
    owner: str = "SYSADMIN"
    comment: str = None


class Share(Resource):
    """
    CREATE [ OR REPLACE ] SHARE [ IF NOT EXISTS ] <name>
      [ COMMENT = '<string_literal>' ]
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
