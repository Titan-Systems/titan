from typing import Dict
from typing_extensions import Annotated

from pydantic import BeforeValidator

from . import Resource
from .base import AccountScoped, DatabaseScoped, serialize_resource_by_name, coerce_from_str
from ..builder import tidy_sql
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

    def create_sql(self, or_replace=False, if_not_exists=False):
        return tidy_sql(
            "CREATE",
            "OR REPLACE" if or_replace else "",
            self.resource_type,
            "IF NOT EXISTS" if if_not_exists else "",
            self.fqn,
            self.props.render(self),
        )

    def drop_sql(self, if_exists=False):
        return tidy_sql(
            "DROP ROLE",
            "IF EXISTS" if if_exists else "",
            self.fqn,
        )


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
