from dataclasses import dataclass

from .resource import Resource, ResourceSpec
from ..enums import ResourceType
from ..scope import SchemaScope

from ..props import (
    BoolProp,
    ColumnNamesProp,
    FlagProp,
    Props,
    QueryProp,
    StringProp,
    TagsProp,
)


@dataclass
class _View(ResourceSpec):
    name: str
    owner: str = "SYSADMIN"
    secure: bool = None
    volatile: bool = None
    recursive: bool = None
    columns: list[dict] = None
    tags: dict[str, str] = None
    change_tracking: bool = None
    copy_grants: bool = None
    comment: str = None
    as_: str = None

    def __post_init__(self):
        super().__post_init__()
        if self.columns is not None and len(self.columns) == 0:
            raise ValueError("columns can't be empty")


class View(Resource):
    resource_type = ResourceType.VIEW
    props = Props(
        secure=FlagProp("secure"),
        volatile=FlagProp("volatile"),
        recursive=FlagProp("recursive"),
        columns=ColumnNamesProp(),
        tags=TagsProp(),
        change_tracking=BoolProp("change_tracking"),  # Not documented
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
        secure: bool = None,
        volatile: bool = None,
        recursive: bool = None,
        columns: list[dict] = None,
        tags: dict[str, str] = None,
        change_tracking: bool = None,
        copy_grants: bool = None,
        comment: str = None,
        as_: str = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._data = _View(
            name=name,
            owner=owner,
            secure=secure,
            volatile=volatile,
            recursive=recursive,
            columns=columns,
            tags=tags,
            change_tracking=change_tracking,
            copy_grants=copy_grants,
            comment=comment,
            as_=as_,
        )
