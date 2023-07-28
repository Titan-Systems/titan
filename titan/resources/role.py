from typing import Dict

from ..resource import Resource, AccountScoped
from ..props import Props, StringProp, TagsProp


class Role(Resource, AccountScoped):
    """
    CREATE [ OR REPLACE ] ROLE [ IF NOT EXISTS ] <name>
      [ [ WITH ] TAG ( <tag_name> = '<tag_value>' [ , <tag_name> = '<tag_value>' , ... ] ) ]
      [ COMMENT = '<string_literal>' ]
    """

    resource_type = "ROLE"
    props = Props(
        tags=TagsProp(),
        comment=StringProp("comment"),
    )

    name: str
    tags: Dict[str, str] = None
    comment: str = None
