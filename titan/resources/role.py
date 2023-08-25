from typing import Dict
from typing_extensions import Annotated

from pydantic import BeforeValidator

from . import Resource
from .base import AccountScoped, DatabaseScoped, serialize_resource_by_name, coerce_from_str
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


T_Role = Annotated[Role, BeforeValidator(coerce_from_str(Role)), serialize_resource_by_name]
