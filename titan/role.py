from typing import Dict

from .resource import Resource, Namespace
from .props import Props, StringProp, TagsProp


class Role(Resource):
    """
    CREATE [ OR REPLACE ] ROLE [ IF NOT EXISTS ] <name>
      [ [ WITH ] TAG ( <tag_name> = '<tag_value>' [ , <tag_name> = '<tag_value>' , ... ] ) ]
      [ COMMENT = '<string_literal>' ]
    """

    resource_type = "ROLE"
    namespace = Namespace.ACCOUNT
    props = Props(
        tags=TagsProp(),
        comment=StringProp("comment"),
    )

    name: str
    tags: Dict[str, str] = {}
    comment: str = None
