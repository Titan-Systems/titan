from dataclasses import dataclass

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
from .tag import TaggableResource
from .warehouse import Warehouse


@dataclass(unsafe_hash=True)
class _Notebook(ResourceSpec):
    name: ResourceName
    version: str = None
    from_: str = None
    main_file: str = None
    comment: str = None
    default_version: str = None
    query_warehouse: Warehouse = None
    tags: dict[str, str] = None


class Notebook(NamedResource, TaggableResource, Resource):
    resource_type = ResourceType.NOTEBOOK
    props = Props(
        version=StringProp("version"),
        from_=StringProp("from"),
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
        version: str = None,
        from_: str = None,
        main_file: str = None,
        comment: str = None,
        default_version: str = None,
        query_warehouse: str = None,
        tags: dict[str, str] = None,
        **kwargs,
    ):
        super().__init__(name, **kwargs)
        self._data: _Notebook = _Notebook(
            name=self._name,
            version=version,
            from_=from_,
            main_file=main_file,
            comment=comment,
            default_version=default_version,
            query_warehouse=query_warehouse,
            tags=tags,
        )
        self.set_tags(tags)
