from dataclasses import dataclass

from .resource import Resource, ResourceSpec
from ..enums import ResourceType
from ..parse import parse_identifier
from ..props import Props, StringProp, TagsProp
from ..resource_name import ResourceName
from ..scope import AccountScope, DatabaseScope


@dataclass(unsafe_hash=True)
class _Role(ResourceSpec):
    name: ResourceName
    owner: str = "USERADMIN"
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

    def __init__(
        self,
        name: str,
        owner: str = "USERADMIN",
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

    @property
    def name(self):
        return self._data.name


class DatabaseRole(Resource):
    """A database role is a special role that is scoped to a single database.

    CREATE [ OR REPLACE ] DATABASE ROLE [ IF NOT EXISTS ] <name>
      [ COMMENT = '<string_literal>' ]
    """

    resource_type = ResourceType.DATABASE_ROLE
    props = Props(
        comment=StringProp("comment"),
    )
    scope = DatabaseScope()
    spec = _Role

    def __init__(
        self,
        name: str,
        database: str = None,
        owner: str = "USERADMIN",
        tags: dict[str, str] = None,
        comment: str = None,
        **kwargs,
    ):
        fqn = parse_identifier(name, is_db_scoped=True)
        if fqn.database:
            database = fqn.database
        super().__init__(database=database, **kwargs)
        self._data: _Role = _Role(
            name=fqn.name,
            owner=owner,
            tags=tags,
            comment=comment,
        )

    @property
    def name(self):
        return self._data.name
