from typing import Dict

from . import Resource
from .base import AccountScoped, DatabaseScoped
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
    owner: str = "SYSADMIN"
    tags: Dict[str, str] = None
    comment: str = None


class DatabaseRole(Resource, DatabaseScoped):
    """
    CREATE [ OR REPLACE ] DATABASE ROLE [ IF NOT EXISTS ] <name>
      [ COMMENT = '<string_literal>' ]
    """

    resource_type = "DATABASE ROLE"
    props = Props(
        comment=StringProp("comment"),
    )

    name: str
    owner: str = "SYSADMIN"
    comment: str = None
