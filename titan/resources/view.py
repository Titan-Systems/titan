from dataclasses import dataclass, field

from ..enums import ResourceType
from ..props import (
    BoolProp,
    ColumnNamesProp,
    FlagProp,
    Props,
    QueryProp,
    StringProp,
    TagsProp,
)
from ..resource_name import ResourceName
from ..role_ref import RoleRef
from ..scope import SchemaScope
from .resource import NamedResource, Resource, ResourceSpec
from .tag import TaggableResource


@dataclass(unsafe_hash=True)
class _View(ResourceSpec):
    name: ResourceName
    owner: RoleRef = "SYSADMIN"
    secure: bool = False
    volatile: bool = None
    recursive: bool = None
    columns: list[dict] = None
    change_tracking: bool = False
    copy_grants: bool = field(default_factory=False, metadata={"fetchable": False})
    comment: str = None
    # TODO: remove this if parsing is feasible
    as_: str = field(default=None, metadata={"fetchable": False})

    def __post_init__(self):
        super().__post_init__()
        if self.columns is not None and len(self.columns) == 0:
            raise ValueError("columns can't be empty")


class View(NamedResource, TaggableResource, Resource):
    """
    Description:
        Represents a view in Snowflake, which is a virtual table created by a stored query on the data.
        Views are used to simplify complex queries, improve security, or enhance performance.

    Snowflake Docs:
        https://docs.snowflake.com/en/sql-reference/sql/create-view

    Fields:
        name (string, required): The name of the view.
        owner (string or Role): The owner role of the view. Defaults to "SYSADMIN".
        secure (bool): Specifies if the view is secure.
        volatile (bool): Specifies if the view is volatile.
        recursive (bool): Specifies if the view is recursive.
        columns (list): A list of dictionaries specifying column details.
        tags (dict): A dictionary of tags associated with the view.
        change_tracking (bool): Specifies if change tracking is enabled.
        copy_grants (bool): Specifies if grants should be copied from the base table.
        comment (string): A comment for the view.
        as_ (string): The SELECT statement defining the view.

    Python:

        ```python
        view = View(
            name="some_view",
            owner="SYSADMIN",
            secure=True,
            as_="SELECT * FROM some_table"
        )
        ```

    Yaml:

        ```yaml
        views:
          - name: some_view
            owner: SYSADMIN
            secure: true
            as_: "SELECT * FROM some_table"
        ```
    """

    resource_type = ResourceType.VIEW
    props = Props(
        columns=ColumnNamesProp(),
        secure=FlagProp("secure"),
        volatile=FlagProp("volatile"),
        recursive=FlagProp("recursive"),
        tags=TagsProp(),
        change_tracking=BoolProp("change_tracking"),
        copy_grants=FlagProp("copy grants"),
        comment=StringProp("comment"),
        as_=QueryProp("as"),
    )
    scope = SchemaScope()
    spec = _View

    def __init__(
        self,
        name: str,
        owner: str = "SYSADMIN",
        secure: bool = False,
        volatile: bool = None,
        recursive: bool = None,
        columns: list[dict] = None,
        tags: dict[str, str] = None,
        change_tracking: bool = False,
        copy_grants: bool = False,
        comment: str = None,
        as_: str = None,
        **kwargs,
    ):
        super().__init__(name, **kwargs)
        self._data: _View = _View(
            name=self._name,
            owner=owner,
            secure=secure,
            volatile=volatile,
            recursive=recursive,
            columns=columns,
            change_tracking=change_tracking,
            copy_grants=copy_grants,
            comment=comment,
            as_=as_,
        )
        self.set_tags(tags)
