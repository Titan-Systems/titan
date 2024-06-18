from dataclasses import dataclass

from .resource import Resource, ResourceSpec, ResourceNameTrait
from .role import Role
from ..enums import AccountEdition, ResourceType
from ..scope import SchemaScope
from ..resource_name import ResourceName
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
    name: ResourceName
    owner: Role = "SYSADMIN"
    secure: bool = False
    columns: list[dict] = None
    tags: dict[str, str] = None
    copy_grants: bool = False
    cluster_by: list[str] = None
    comment: str = None
    as_: str = None


class MaterializedView(ResourceNameTrait, Resource):
    """
    Description:
        A Materialized View in Snowflake is a database object that contains the results of a query.
        It is physically stored and automatically updated as data changes, providing faster access to data.

    Snowflake Docs:
        https://docs.snowflake.com/en/sql-reference/sql/create-materialized-view

    Fields:
        name (string, required): The name of the materialized view.
        owner (string or Role): The owner role of the materialized view. Defaults to "SYSADMIN".
        secure (bool): Specifies if the materialized view is secure. Defaults to False.
        columns (list): A list of dictionaries specifying column definitions.
        tags (dict): Tags associated with the materialized view.
        copy_grants (bool): Specifies if grants should be copied from the source. Defaults to False.
        comment (string): A comment for the materialized view.
        cluster_by (list): A list of expressions defining the clustering of the materialized view.
        as_ (string, required): The SELECT statement used to populate the materialized view.

    Python:

        ```python
        materialized_view = MaterializedView(
            name="some_materialized_view",
            owner="SYSADMIN",
            secure=True,
            as_="SELECT * FROM some_table",
        )
        ```

    Yaml:

        ```yaml
        materialized_views:
          - name: some_materialized_view
            owner: SYSADMIN
            secure: true
            as: SELECT * FROM some_table
        ```
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
        super().__init__(name, **kwargs)
        self._data: _MaterializedView = _MaterializedView(
            name=self._name,
            owner=owner,
            cluster_by=cluster_by,
            secure=secure,
            columns=columns,
            tags=tags,
            copy_grants=copy_grants,
            comment=comment,
            as_=as_,
        )
