from typing import Dict
from typing_extensions import Annotated

from pydantic import BeforeValidator

from .base import (
    AccountScoped,
    DatabaseScoped,
    Resource,
    _fix_class_documentation,
    serialize_resource_by_name,
    coerce_from_str,
)
from ..privs import GlobalPriv, Privs, RolePriv
from ..props import Props, StringProp, TagsProp


@_fix_class_documentation
class Role(AccountScoped, Resource):
    """
    CREATE [ OR REPLACE ] ROLE [ IF NOT EXISTS ] <name>
      [ [ WITH ] TAG ( <tag_name> = '<tag_value>' [ , <tag_name> = '<tag_value>' , ... ] ) ]
      [ COMMENT = '<string_literal>' ]
    """

    resource_type = "ROLE"
    lifecycle_privs = Privs(
        create=GlobalPriv.CREATE_ROLE,
        delete=RolePriv.OWNERSHIP,
    )
    props = Props(
        tags=TagsProp(),
        comment=StringProp("comment"),
    )

    name: str
    owner: str = "SYSADMIN"
    tags: Dict[str, str] = None
    comment: str = None


class DatabaseRole(DatabaseScoped, Resource):
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
