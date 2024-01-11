from dataclasses import dataclass

from .__resource import Resource, ResourceSpec
from ..enums import ResourceType
from ..props import Props, StringProp, TagsProp
from ..scope import AccountScope, DatabaseScope


@dataclass
class _Role(ResourceSpec):
    name: str
    owner: str = "SYSADMIN"
    tags: dict[str, str] = None
    comment: str = None


class Role(Resource):
    resource_type = ResourceType.ROLE
    props = Props(
        tags=TagsProp(),
        comment=StringProp("comment"),
    )
    scope = AccountScope()
    spec = _Role

    name: str
    owner: str = "SYSADMIN"
    tags: dict[str, str] = None
    comment: str = None

    def __init__(
        self,
        name: str,
        owner: str = "SYSADMIN",
        tags: dict[str, str] = None,
        comment: str = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._data = _Role(
            name=name,
            owner=owner,
            tags=tags,
            comment=comment,
        )


class DatabaseRole(Resource):
    resource_type = ResourceType.DATABASE_ROLE
    props = Props(
        comment=StringProp("comment"),
    )
    scope = DatabaseScope()
    spec = _Role

    name: str
    owner: str = "SYSADMIN"
    tags: dict[str, str] = None
    comment: str = None

    def __init__(
        self,
        name: str,
        owner: str = "SYSADMIN",
        tags: dict[str, str] = None,
        comment: str = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._data = _Role(
            name=name,
            owner=owner,
            tags=tags,
            comment=comment,
        )
