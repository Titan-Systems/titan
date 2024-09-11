from dataclasses import dataclass

from ..enums import ResourceType
from ..props import Props, StringProp, TagsProp
from ..resource_name import ResourceName
from ..scope import AccountScope, DatabaseScope
from .resource import NamedResource, Resource, ResourcePointer, ResourceSpec
from .tag import TaggableResource


@dataclass(unsafe_hash=True)
class _Role(ResourceSpec):
    name: ResourceName
    owner: ResourceName = "USERADMIN"
    comment: str = None

    def __post_init__(self):
        super().__post_init__()
        self.owner = ResourcePointer(self.owner, ResourceType.ROLE)


class Role(NamedResource, TaggableResource, Resource):
    """
    Description:
        A role in Snowflake defines a set of access controls and permissions.

    Snowflake Docs:
        https://docs.snowflake.com/en/sql-reference/sql/create-role

    Fields:
        name (string, required): The name of the role.
        owner (string): The owner of the role. Defaults to "USERADMIN".
        tags (dict): Tags associated with the role.
        comment (string): A comment for the role.

    Python:

        ```python
        role = Role(
            name="some_role",
            owner="USERADMIN",
            comment="This is a sample role.",
        )
        ```

    Yaml:

        ```yaml
        roles:
          - name: some_role
            owner: USERADMIN
            comment: This is a sample role.
        ```
    """

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
        super().__init__(name, **kwargs)
        self._data = _Role(
            name=self._name,
            owner=owner,
            comment=comment,
        )
        self.set_tags(tags)


class DatabaseRole(NamedResource, TaggableResource, Resource):
    """
    Description:
        A database role in Snowflake is a collection of privileges that can be assigned to users or other roles within a specific database context. It is used to manage access control and permissions at the database level.

    Snowflake Docs:
        https://docs.snowflake.com/en/sql-reference/sql/create-database-role

    Fields:
        name (string, required): The name of the database role.
        database (string): The database this role is associated with. This is derived from the fully qualified name.
        owner (string): The owner of the database role. Defaults to "USERADMIN".
        tags (dict): Tags associated with the database role.
        comment (string): A comment about the database role.

    Python:

        ```python
        database_role = DatabaseRole(
            name="some_database_role",
            database="some_database",
            owner="USERADMIN",
            tags={"department": "finance"},
            comment="This role is for database-specific access control."
        )
        ```

    Yaml:

        ```yaml
        database_roles:
          - name: some_database_role
            database: some_database
            owner: USERADMIN
            tags:
              department: finance
            comment: This role is for database-specific access control.
        ```
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
        database: str,
        owner: str = "USERADMIN",
        tags: dict[str, str] = None,
        comment: str = None,
        **kwargs,
    ):
        super().__init__(name, database=database, **kwargs)
        self._data: _Role = _Role(
            name=self._name,
            owner=owner,
            comment=comment,
        )
        self.set_tags(tags)

    def to_dict(self):
        data = super().to_dict()
        data["database"] = self.container.name
        return data
