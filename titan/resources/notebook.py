from dataclasses import dataclass, field

from ..enums import ResourceType
from ..props import (
    IdentifierProp,
    Props,
    StringProp,
    TagsProp,
)
from ..resource_name import ResourceName
from ..scope import SchemaScope
from .resource import NamedResource, Resource, ResourceSpec
from ..role_ref import RoleRef
from .warehouse import Warehouse

# TODO: I can't get version to work at all with Snowflake, I suspect it's buggy.


@dataclass(unsafe_hash=True)
class _Notebook(ResourceSpec):
    name: ResourceName
    # version: str = None
    from_: str = field(default=None, metadata={"fetchable": False})
    main_file: str = None
    comment: str = None
    default_version: str = None
    query_warehouse: Warehouse = None
    owner: RoleRef = "SYSADMIN"


class Notebook(NamedResource, Resource):
    resource_type = ResourceType.NOTEBOOK
    props = Props(
        # version=StringProp("version", eq=False),
        from_=StringProp("from", eq=False),
        main_file=StringProp("main_file"),
        comment=StringProp("comment"),
        default_version=StringProp("default_version"),
        query_warehouse=IdentifierProp("query_warehouse"),
        tags=TagsProp(),
    )
    scope = SchemaScope()
    spec = _Notebook

    def __init__(
        self,
        name: str,
        # version: str = None,
        from_: str = None,
        main_file: str = None,
        comment: str = None,
        default_version: str = None,
        query_warehouse: str = None,
        owner: str = "SYSADMIN",
        **kwargs,
    ):
        super().__init__(name, **kwargs)
        self._data: _Notebook = _Notebook(
            name=self._name,
            # version=version,
            from_=from_,
            main_file=main_file,
            comment=comment,
            default_version=default_version,
            query_warehouse=query_warehouse,
            owner=owner,
        )
