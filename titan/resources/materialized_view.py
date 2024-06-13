from dataclasses import dataclass

from .resource import Resource, ResourceSpec
from .role import Role
from ..enums import AccountEdition, ResourceType
from ..scope import SchemaScope

from ..props import (
    BoolProp,
    ColumnNamesProp,
    FlagProp,
    IdentifierListProp,
    Props,
    QueryProp,
    StringProp,
    TagsProp,
)


@dataclass(unsafe_hash=True)
class _MaterializedView(ResourceSpec):
    name: str
    owner: Role = "SYSADMIN"
    secure: bool = False
    columns: list[dict] = None
    tags: dict[str, str] = None
    copy_grants: bool = False
    cluster_by: list[str] = None
    comment: str = None
    as_: str = None


class MaterializedView(Resource):
    """
    CREATE [ OR REPLACE ] [ SECURE ] MATERIALIZED VIEW [ IF NOT EXISTS ] <name>
    [ COPY GRANTS ]
    ( <column_list> )
    [ <col1> [ WITH ] MASKING POLICY <policy_name> [ USING ( <col1> , <cond_col1> , ... ) ]
            [ WITH ] PROJECTION POLICY <policy_name>
            [ WITH ] TAG ( <tag_name> = '<tag_value>' [ , <tag_name> = '<tag_value>' , ... ] ) ]
    [ , <col2> [ ... ] ]
    [ COMMENT = '<string_literal>' ]
    [ [ WITH ] ROW ACCESS POLICY <policy_name> ON ( <col_name> [ , <col_name> ... ] ) ]
    [ [ WITH ] AGGREGATION POLICY <policy_name> [ ENTITY KEY ( <col_name> [ , <col_name> ... ] ) ] ]
    [ [ WITH ] TAG ( <tag_name> = '<tag_value>' [ , <tag_name> = '<tag_value>' , ... ] ) ]
    [ CLUSTER BY ( <expr1> [, <expr2> ... ] ) ]
    AS <select_statement>
    """

    edition = {AccountEdition.ENTERPRISE, AccountEdition.BUSINESS_CRITICAL}
    resource_type = ResourceType.MATERIALIZED_VIEW
    props = Props(
        columns=ColumnNamesProp(),
        secure=FlagProp("secure"),
        copy_grants=FlagProp("copy grants"),
        comment=StringProp("comment"),
        tags=TagsProp(),
        cluster_by=IdentifierListProp("cluster by", eq=False, parens=True),
        as_=QueryProp("as"),
    )
    scope = SchemaScope()
    spec = _MaterializedView

    def __init__(
        self,
        name: str,
        owner: str = "SYSADMIN",
        secure: bool = False,
        columns: list[dict] = None,
        tags: dict[str, str] = None,
        copy_grants: bool = False,
        comment: str = None,
        cluster_by: list[str] = None,
        as_: str = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._data = _MaterializedView(
            name=name,
            owner=owner,
            cluster_by=cluster_by,
            secure=secure,
            columns=columns,
            tags=tags,
            copy_grants=copy_grants,
            comment=comment,
            as_=as_,
        )
