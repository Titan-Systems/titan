from dataclasses import asdict, dataclass, fields
from typing import Any, TypedDict, Type, get_args, get_origin
from inspect import isclass
from itertools import chain

import pyparsing as pp

from ..enums import DataType, ParseableEnum, ResourceType
from ..identifiers import URN
from ..lifecycle import create_resource, drop_resource
from ..props import Props as ResourceProps
from ..parse import _parse_create_header, _parse_props
from ..scope import ResourceScope, OrganizationScope, DatabaseScope, SchemaScope


class Arg(TypedDict):
    name: str
    data_type: DataType


class Returns(TypedDict):
    data_type: DataType
    metadata: str


@dataclass
class ResourceSpec:
    def __post_init__(self):
        def _coerce(field_value, field_type):
            if field_type == Any:
                return field_value

            # Recursively traverse lists and dicts
            if get_origin(field_type) == list:  # or issubclass(field_type, list):
                if not isinstance(field_value, list):
                    raise Exception
                list_element_type = get_args(field_type) or (str,)
                return [_coerce(v, field_type=list_element_type[0]) for v in field_value]
            elif get_origin(field_type) == dict:  # issubclass(field_type, dict):
                if not isinstance(field_value, dict):
                    raise Exception
                dict_types = get_args(field_type)
                if len(dict_types) < 2:
                    raise RuntimeError(f"Unexpected field type {field_type}")
                return {k: _coerce(v, field_type=dict_types[1]) for k, v in field_value.items()}

            if not isclass(field_type):
                raise RuntimeError(f"Unexpected field type {field_type}")

            # Coerce enums
            if issubclass(field_type, ParseableEnum):
                return field_type(field_value)

            # Coerce args
            elif field_type == Arg:
                arg_dict = {
                    "name": field_value["name"].upper(),
                    "data_type": DataType(field_value["data_type"]),
                }
                if "default" in field_value:
                    arg_dict["default"] = field_value["default"]
                return arg_dict

            # Coerce returns
            elif field_type == Returns:
                returns_dict = {
                    "data_type": DataType(field_value["data_type"]),
                    "metadata": field_value["metadata"],
                }
                if "returns_null" in field_value:
                    returns_dict["returns_null"] = field_value["returns_null"]
                return returns_dict

            # Coerce resources
            elif issubclass(field_type, Resource):
                if isinstance(field_value, str):
                    return ResourcePointer(name=field_value, resource_type=field_type.resource_type)
                elif isinstance(field_value, dict):
                    # We permit two types of anonymous resource declarations in the constructor of another resource:
                    # 1. A dict with a single key, the name of the resource
                    # Example:
                    #   grant = Grant(priv='SELECT', on='table', to={'name': 'some_role'})
                    # 2. A dict representing a column or column-like object
                    # Example:
                    #   table = Table(name='my_table', columns=[{'name': 'id', 'data_type': 'int'}])

                    if field_type.__name__ == "Column":
                        return field_type(**field_value)
                    else:
                        return ResourcePointer(**field_value, resource_type=field_type.resource_type)
                elif isinstance(field_value, field_type):
                    return field_value
            else:
                return field_value

        for field in fields(self):
            field_value = getattr(self, field.name)

            if field_value is None:
                continue
            # elif isinstance(field.type, _GenericAlias):
            #     continue
            else:
                setattr(self, field.name, _coerce(field_value, field.type))


RESOURCE_SCOPES = {
    ResourceType.ACCOUNT: OrganizationScope(),
}


class _Resource(type):
    __types__ = {}
    __resolvers__ = {}

    def __new__(cls, name, bases, attrs):
        cls_ = super().__new__(cls, name, bases, attrs)
        if cls_.__name__ in ["Resource", "_Resource", "ResourcePointer"]:
            return cls_
        if cls_.resource_type not in cls.__types__:
            cls.__types__[cls_.resource_type] = []
        cls.__types__[cls_.resource_type].append(cls_)
        if cls_.resource_type not in RESOURCE_SCOPES:
            RESOURCE_SCOPES[cls_.resource_type] = cls_.scope
        return cls_


class Resource(metaclass=_Resource):
    props: ResourceProps
    resource_type: ResourceType
    scope: ResourceScope
    spec: Type[ResourceSpec]

    def __init__(self, implicit: bool = False, stub: bool = False, **scope_kwargs):
        super().__init__()
        self._data: ResourceSpec = None
        self._container: "ResourceContainer" = None
        self.implicit = implicit
        self.refs = set()
        self._register_scope(**scope_kwargs)
        if stub:
            raise Exception("Stub resources are deprecated")

    @classmethod
    def from_sql(cls, sql):
        resource_cls = cls
        if resource_cls == Resource:
            # FIXME: we need to change the way we handle polymorphic resources
            # make a new function called _parse_resource_type_from_create
            # resource_cls = Resource.classes[_resolve_resource_class(sql)]
            raise NotImplementedError

        identifier, remainder_sql = _parse_create_header(sql, resource_cls)

        try:
            props = _parse_props(resource_cls.props, remainder_sql) if remainder_sql else {}
            return resource_cls(**identifier, **props)
        except pp.ParseException as err:
            raise pp.ParseException(f"Error parsing {resource_cls.__name__} props {identifier}") from err

    @classmethod
    def props_for_resource_type(cls, resource_type: ResourceType, data: dict = None):
        return cls.resolve_resource_cls(resource_type, data).props

    @classmethod
    def resolve_resource_cls(cls, resource_type: ResourceType, data: dict = None) -> Type["Resource"]:

        if not isinstance(resource_type, ResourceType):
            raise ValueError(f"Expected ResourceType, got {resource_type}({type(resource_type)})")
        if resource_type not in cls.__types__:
            raise ValueError(f"Resource class for type not found {resource_type}")
        resource_types = cls.__types__[resource_type]

        if len(resource_types) > 1:
            if data is None:
                raise ValueError(f"Cannot resolve polymorphic resource class [{resource_type}] without data")
            else:
                resolver = cls.__resolvers__[resource_type]
                return resolver(data)
        return resource_types[0]

    @classmethod
    def defaults(cls):
        return {f.name: f.default for f in fields(cls.spec)}

    def __repr__(self):  # pragma: no cover
        return f"{self.__class__.__name__}({str(self.fqn)})"

    def to_dict(self, packed=False):
        defaults = {f.name: f.default for f in fields(self.spec)}

        serialized = {}

        def _serialize(value):
            if isinstance(value, ResourcePointer):
                return value.name
            elif isinstance(value, Resource):
                if hasattr(value, "serialize"):
                    return value.serialize()
                elif hasattr(value._data, "name"):
                    return getattr(value._data, "name")
                else:
                    raise Exception(f"Cannot serialize {value}")
            elif isinstance(value, ParseableEnum):
                return str(value)
            elif isinstance(value, list):
                return [_serialize(v) for v in value]
            elif isinstance(value, dict):
                return {k: _serialize(v) for k, v in value.items()}
            else:
                return value

        for key, value in asdict(self._data).items():
            skip_field = (value is None) or (value == defaults[key])
            skip_field = skip_field and (key != "owner")
            if skip_field:
                # continue
                pass
            serialized[key] = _serialize(value)

        return serialized

    def create_sql(self, **kwargs):
        return create_resource(
            self.urn,
            self.to_dict(packed=True),
            self.props,
            **kwargs,
        )

    def drop_sql(self, if_exists: bool = False):
        return drop_resource(self.urn, self.to_dict(), if_exists=if_exists)

    def _requires(self, resource: "Resource"):
        if isinstance(resource, (Resource, ResourcePointer)):
            self.refs.add(resource)

    def requires(self, *resources: "Resource"):
        if isinstance(resources[0], list):
            resources = resources[0]
        for resource in resources:
            self._requires(resource)

    def _register_scope(self, database=None, schema=None):
        if isinstance(database, str):
            database: ResourceContainer = ResourcePointer(name=database, resource_type=ResourceType.DATABASE)

        if isinstance(schema, str):
            schema: ResourceContainer = ResourcePointer(name=schema, resource_type=ResourceType.SCHEMA)

        if isinstance(self.scope, DatabaseScope):
            if schema is not None:
                raise RuntimeError(f"Unexpected kwarg schema {schema} for resource {self}")
            if database is not None:
                database.add(self)

        if isinstance(self.scope, SchemaScope):
            if schema is not None:
                schema.add(self)

    @property
    def container(self):
        return self._container

    @property
    def fqn(self):
        name = getattr(self._data, "name")
        return self.scope.fully_qualified_name(self.container, name)

    @property
    def urn(self):
        return URN.from_resource(self, account_locator="")


class ResourceContainer:
    def __init__(self):
        self._items: dict[ResourceType, list[Resource]] = {}

    def add(self, *items: Resource):
        if isinstance(items[0], list):
            items = items[0]
        for item in items:

            if item.container and not isinstance(item.container, ResourcePointer):
                raise RuntimeError(f"{item} already belongs to a container")
            item._container = self
            item.requires(self)
            if item.resource_type not in self._items:
                self._items[item.resource_type] = []
            self._items[item.resource_type].append(item)

    def items(self, resource_type: ResourceType = None) -> list[Resource]:
        if resource_type:
            return self._items.get(resource_type, [])
        else:
            return list(chain.from_iterable(self._items.values()))

    def find(self, resource_type: ResourceType, name: str) -> Resource:
        for resource in self.items(resource_type):
            if resource._data.name == name:
                return resource
        raise KeyError(f"Resource {resource_type} {name} not found")

    @property
    def name(self):
        return self._data.name


class ResourcePointer(Resource, ResourceContainer):
    def __init__(self, name: str, resource_type: ResourceType):

        self._name: str = name
        self._resource_type: ResourceType = resource_type
        self.scope = RESOURCE_SCOPES[resource_type]
        super().__init__()

    def __repr__(self):  # pragma: no cover
        resource_type = getattr(self, "resource_type", None)
        name = getattr(self, "name", None)
        return f"ResourcePointer({resource_type}:{name})"

    @property
    def container(self):
        return self._container

    @property
    def fqn(self):
        return self.scope.fully_qualified_name(self.container, self.name)

    @property
    def name(self):
        return self._name

    @property
    def resource_type(self):
        return self._resource_type

    def to_dict(self, packed=False):
        return {"name": self.name}
