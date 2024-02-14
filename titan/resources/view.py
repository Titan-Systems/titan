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


@dataclass(unsafe_hash=True)
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
    """
    CREATE
      [ OR REPLACE ]
      [ SECURE ]
      [ { [ { LOCAL | GLOBAL } ] TEMP | TEMPORARY | VOLATILE } ]
      [ RECURSIVE ]
      VIEW [ IF NOT EXISTS ] <name>
      [ ( <column_list> ) ]
      [ <col1> [ WITH ] MASKING POLICY <policy_name> [ USING ( <col1> , <cond_col1> , ... ) ]
               [ WITH ] TAG ( <tag_name> = '<tag_value>' [ , <tag_name> = '<tag_value>' , ... ] ) ]
      [ , <col2> [ ... ] ]
      [ [ WITH ] ROW ACCESS POLICY <policy_name> ON ( <col_name> [ , <col_name> ... ] ) ]
      [ [ WITH ] TAG ( <tag_name> = '<tag_value>' [ , <tag_name> = '<tag_value>' , ... ] ) ]
      [ COPY GRANTS ]
      [ COMMENT = '<string_literal>' ]
      AS <select_statement>
    """

    resource_type = ResourceType.VIEW
    props = Props(
        columns=ColumnNamesProp(),
        secure=FlagProp("secure"),
        volatile=FlagProp("volatile"),
        recursive=FlagProp("recursive"),
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
