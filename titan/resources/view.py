from typing import Dict, List

from pydantic import field_validator

from .base import Resource, SchemaScoped
from ..props import (
    BoolProp,
    ColumnNamesProp,
    FlagProp,
    Props,
    QueryProp,
    StringProp,
    TagsProp,
)


class View(Resource, SchemaScoped):
    """
    CREATE [ OR REPLACE ] [ SECURE ] [ { [ { LOCAL | GLOBAL } ] TEMP | TEMPORARY | VOLATILE } ] [ RECURSIVE ] VIEW [ IF NOT EXISTS ] <name>
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

    resource_type = "VIEW"
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

    name: str
    owner: str = "SYSADMIN"

    secure: bool = None
    volatile: bool = None
    recursive: bool = None
    columns: List[dict] = None
    tags: Dict[str, str] = None
    change_tracking: bool = None
    copy_grants: bool = None
    comment: str = None
    as_: str

    @field_validator("columns")
    @classmethod
    def validate_columns(cls, columns):
        if isinstance(columns, list):
            assert len(columns) > 0, "columns must not be empty"
        return columns
