from typing import Dict

from pydantic import Field

from titan.resource import Resource, SchemaScoped

from titan.props import (
    Props,
    FlagProp,
    QueryProp,
    ResourceListProp,
    StringProp,
    TagsProp,
)

from .column import Column

# from .schema import Schema


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
        columns=ResourceListProp(Column),
        volatile=FlagProp("volatile"),
        recursive=FlagProp("recursive"),
        tags=TagsProp(),
        copy_grants=FlagProp("copy grants"),
        comment=StringProp("comment"),
        as_=QueryProp("as"),
    )

    name: str
    owner: str = None

    secure: bool = None
    columns: list = []
    volatile: bool = False
    recursive: bool = False
    tags: Dict[str, str] = None
    copy_grants: bool = False
    comment: str = None
    as_: str = None
