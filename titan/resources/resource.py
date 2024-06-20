import sys
import types

import difflib

from dataclasses import dataclass, fields
from typing import Any, TypedDict, Type, Union, get_args, get_origin
from inspect import isclass
from itertools import chain

import pyparsing as pp

from ..enums import AccountEdition, DataType, ParseableEnum, ResourceType
from ..identifiers import URN
from ..lifecycle import create_resource, drop_resource
from ..props import Props as ResourceProps
from ..parse import _parse_create_header, _parse_props, _resolve_resource_class, parse_identifier
from ..resource_name import ResourceName
from ..resource_tags import ResourceTags
from ..scope import (
    AccountScope,
    DatabaseScope,
    OrganizationScope,
    ResourceScope,
    SchemaScope,
)


def _suggest_correct_kwargs(expected_kwargs, passed_kwargs):
    suggestions = {}
    for passed_kwarg in passed_kwargs:
        # Find the closest match from the expected kwargs for each passed kwarg
        closest_match = difflib.get_close_matches(passed_kwarg, expected_kwargs, n=1, cutoff=0.6)
        if closest_match:
            closest_match = closest_match[0]
            # If the passed kwarg is not exactly an expected kwarg, add it to suggestions
            if passed_kwarg != closest_match:
                suggestions[passed_kwarg] = closest_match
        else:
            # If no close match is found, suggest it might be an unexpected kwarg
            suggestions[passed_kwarg] = "Unexpected kwarg, no close match found."

    return suggestions


class Arg(TypedDict):
    name: str
    data_type: DataType


class Returns(TypedDict):
    data_type: DataType
    metadata: str


def _coerce_resource_field(field_value, field_type):
    if field_type == Any:
        return field_value

    # Recursively traverse lists and dicts
    if get_origin(field_type) == list:
        if not isinstance(field_value, list):
            raise Exception
        list_element_type = get_args(field_type) or (str,)
        return [_coerce_resource_field(v, field_type=list_element_type[0]) for v in field_value]
    elif get_origin(field_type) == dict:
        if not isinstance(field_value, dict):
            raise Exception
        dict_types = get_args(field_type)
        if len(dict_types) < 2:
            raise RuntimeError(f"Unexpected field type {field_type}")
        return {k: _coerce_resource_field(v, field_type=dict_types[1]) for k, v in field_value.items()}

    # Check for field_value's type in a Union
    if get_origin(field_type) == Union:
        union_types = get_args(field_type)
        for union_type in union_types:
            expected_type = get_origin(union_type) or union_type
            if isinstance(field_value, expected_type):
                return _coerce_resource_field(field_value, field_type=expected_type)
        raise RuntimeError(f"Unexpected field type {field_type}")

    if not isclass(field_type):
        # If we want to support quoted type annotations, this is the place to do it.
        # eg owner "Role" = "SYSADMIN"
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
        return convert_to_resource(field_type, field_value)
    elif field_type == ResourceName:
        return ResourceName(field_value)
    elif field_type == ResourceTags:
        return ResourceTags(field_value)
    else:
        return field_value


@dataclass
class ResourceSpec:
    def __post_init__(self):
        for field in fields(self):
            field_value = getattr(self, field.name)
            if field_value is None:
                continue
            else:
                new_value = _coerce_resource_field(field_value, field.type)
                setattr(self, field.name, new_value)

    @classmethod
    def get_metadata(cls, field_name: str):
        return {f.name: f.metadata for f in fields(cls)}[field_name]


RESOURCE_SCOPES = {
    ResourceType.ACCOUNT: OrganizationScope(),
}


class _Resource(type):
    __types__: dict[ResourceType, list[Type["Resource"]]] = {}
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
    edition = {AccountEdition.STANDARD, AccountEdition.ENTERPRISE, AccountEdition.BUSINESS_CRITICAL}
    props: ResourceProps
    resource_type: ResourceType
    scope: ResourceScope
    spec: Type[ResourceSpec]
    serialize_inline: bool = False

    def __init__(self, implicit: bool = False, **kwargs):
        super().__init__()
        self._data: ResourceSpec = None
        self._container: "ResourceContainer" = None
        self.implicit = implicit
        self.refs = set()

        # Consume resource_type from kwargs if it exists
        resource_type = kwargs.pop("resource_type", None)
        resource_type = ResourceType(resource_type) if resource_type else None
        if resource_type and resource_type != self.resource_type:
            raise ValueError(f"Unexpected resource_type {resource_type} for {self.resource_type}")

        # Consume scope from kwargs if it exists
        database = kwargs.pop("database", None)
        schema = kwargs.pop("schema", None)
        self._register_scope(database=database, schema=schema)

        # If there are more kwargs, throw an error
        # Based on https://stackoverflow.com/questions/1603940/how-can-i-modify-a-python-traceback-object-when-raising-an-exception
        if kwargs:
            try:
                if self.spec:
                    field_names = [field.name for field in fields(self.spec)]
                    field_names = ", ".join(field_names)
                    raise ValueError(
                        f"Unexpected keyword arguments for {self.__class__.__name__} {kwargs}. Valid field names: {field_names}"
                    )
                else:
                    raise ValueError(f"Unexpected keyword arguments for {self.__class__.__name__} {kwargs}")
            except ValueError as err:
                traceback = sys.exc_info()[2]
                back_frame = traceback.tb_frame.f_back
                msg = str(err)

            back_tb = types.TracebackType(
                tb_next=None, tb_frame=back_frame, tb_lasti=back_frame.f_lasti, tb_lineno=back_frame.f_lineno
            )
            raise ValueError(msg).with_traceback(back_tb)

    @classmethod
    def from_sql(cls, sql):
        resource_cls = cls
        if resource_cls == Resource:
            # FIXME: we need to change the way we handle polymorphic resources
            # make a new function called _parse_resource_type_from_create
            # resource_cls = Resource.classes[_resolve_resource_class(sql)]
            # raise NotImplementedError
            resource_type = _resolve_resource_class(sql)
            scope = RESOURCE_SCOPES[resource_type]
        else:
            resource_type = resource_cls.resource_type
            scope = resource_cls.scope

        identifier, remainder_sql = _parse_create_header(sql, resource_type, scope)

        try:
            props = _parse_props(resource_cls.props, remainder_sql) if remainder_sql else {}
            return resource_cls(**identifier, **props)
        except pp.ParseException as err:
            raise pp.ParseException(f"Error parsing {resource_cls.__name__} props {identifier}") from err

    @classmethod
    def from_dict(cls, data: dict):
        resource_cls = cls.resolve_resource_cls(ResourceType(data["resource_type"]), data)
        return resource_cls(**data)

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

    # def __copy__(self):
    #     cls = self.__class__
    #     result = cls.__new__(cls)
    #     result.__dict__.update(self.__dict__)
    #     return result

    # def __deepcopy__(self, memo):
    #     cls = self.__class__
    #     print(f"[DEEPCOPY] >> {cls.__name__}")
    #     result = cls.__new__(cls)
    #     memo[id(self)] = result
    #     for k, v in self.__dict__.items():
    #         setattr(result, k, deepcopy(v, memo))
    #     return result

    def __repr__(self):  # pragma: no cover
        name = getattr(self._data, "name", None)
        return f"{self.__class__.__name__}({name})"

    def __eq__(self, other):
        if not isinstance(other, Resource):
            return False
        return self._data == other._data

    def __hash__(self):
        return hash(self._data)

    def to_dict(self):
        serialized = {}

        def _serialize(value):
            if isinstance(value, ResourcePointer):
                return str(value.fqn)
            elif isinstance(value, Resource):
                # if hasattr(value, "serialize"):
                #     return value.serialize()
                if getattr(value, "serialize_inline", False):
                    return value.to_dict()
                if hasattr(value._data, "name"):
                    return getattr(value._data, "name")
                else:
                    raise Exception(f"Cannot serialize {value}")
            elif isinstance(value, ParseableEnum):
                return str(value)
            elif isinstance(value, list):
                return [_serialize(v) for v in value]
            elif isinstance(value, dict):
                return {k: _serialize(v) for k, v in value.items()}
            elif isinstance(value, ResourceName):
                return value._name
            else:
                return value

        # for key, value in asdict(self._data).items():
        #     serialized[key] = _serialize(value)
        for field in fields(self._data):
            value = getattr(self._data, field.name)
            serialized[field.name] = _serialize(value)

        return serialized

    def create_sql(self, **kwargs):
        return create_resource(
            self.urn,
            self.to_dict(),
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
            if database is not None:
                database.add(schema)

        if isinstance(self.scope, DatabaseScope):
            if schema is not None:
                raise RuntimeError(f"Unexpected kwarg schema {schema} for resource {self}")
            if database is not None:
                database.add(self)

        if isinstance(self.scope, SchemaScope):
            if schema is not None:
                schema.add(self)
            elif database is not None:
                database.find(name="PUBLIC", resource_type=ResourceType.SCHEMA).add(self)

    @property
    def container(self):
        return self._container

    @property
    def fqn(self):
        name = getattr(self._data, "name")
        if isinstance(name, ResourceName):
            name = name._name
        return self.scope.fully_qualified_name(self.container, name)

    @property
    def urn(self):
        return URN.from_resource(self, account_locator="")


class ResourceContainer:
    def __init__(self):
        self._items: dict[ResourceType, list[Resource]] = {}

    def __contains__(self, item: Resource):
        return item in self._items.get(item.resource_type, [])

    def add(self, *items: Resource):
        if isinstance(items[0], list):
            items = items[0]
        for item in items:

            if item.container is not None and not isinstance(item.container, ResourcePointer):
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
            if isinstance(resource, ResourcePointer) and resource.name == name:
                return resource
            elif isinstance(resource, Resource) and resource._data is not None and resource._data.name == name:
                return resource
        raise KeyError(f"Resource {resource_type} {name} not found")

    def remove(self, resource: Resource):
        if resource.resource_type in self._items:
            self._items[resource.resource_type].remove(resource)
            resource._container = None
            resource.refs.remove(self)


class ResourceNameTrait:
    """
    This class is a mixin that allows resources to be constructed with fully qualified names
    without a bunch of user-land cruft to make it all work. This keeps titan close to the behavior
    of snowflake.

    For example:

    Without this trait, a scoped resource would need to be defined as:
    >>> tbl = Table(name="TBL", database="DB", schema="SCHEMA")

    With this trait, the same table can be defined this way instead:
    >>> tbl = Table(name="DB.SCHEMA.TBL")
    """

    def __init__(self, name: str, **kwargs):
        name = str(name)

        if isinstance(self.scope, AccountScope):
            self._name = ResourceName(name)
            super().__init__(**kwargs)
            return

        try:
            fqn = parse_identifier(name, is_db_scoped=isinstance(self.scope, DatabaseScope))
        except pp.ParseException as err:
            # Allow identifiers that should be quoted, so long as they aren't insane
            if "." in name:
                raise ValueError(f"Resource name not supported {name}")
            fqn = parse_identifier(f'"{name}"', is_db_scoped=isinstance(self.scope, DatabaseScope))

        self._name = ResourceName(fqn.name)
        if fqn.database and "database" in kwargs:
            raise ValueError("Multiple database names found")
        if fqn.schema and "schema" in kwargs:
            raise ValueError("Multiple schema names found")

        super().__init__(
            database=kwargs.pop("database", fqn.database),
            schema=kwargs.pop("schema", fqn.schema),
            **kwargs,
        )

    @property
    def name(self):
        return self._name


class ResourcePointer(ResourceNameTrait, Resource, ResourceContainer):
    def __init__(self, name: str, resource_type: ResourceType):
        self._resource_type: ResourceType = resource_type
        self.scope = RESOURCE_SCOPES[resource_type]
        super().__init__(name)

        # Don't want to do this for all implicit resources but making an exception for PUBLIC schema
        # If this points to a database, assume it includes a PUBLIC schema
        if self._resource_type == ResourceType.DATABASE and self._name != "SNOWFLAKE":
            self.add(ResourcePointer(name="PUBLIC", resource_type=ResourceType.SCHEMA))
            # self.add(ResourcePointer(name="INFORMATION_SCHEMA", resource_type=ResourceType.SCHEMA))

    def __copy__(self):
        raise Exception
        return ResourcePointer(self._name, self._resource_type)

    def __deepcopy__(self, memo):
        raise Exception
        result = ResourcePointer(self._name, self._resource_type)
        memo[id(self)] = result
        return result

    def __repr__(self):  # pragma: no cover
        resource_type = getattr(self, "resource_type", None)
        name = getattr(self, "name", None)
        return f"<→{resource_type}:{name}>"

    def __eq__(self, other):
        if not isinstance(other, ResourcePointer):
            return False
        return self.name == other.name and self.resource_type == other.resource_type

    def __hash__(self):
        return hash((self._name, self._resource_type))

    @property
    def container(self):
        return self._container

    @property
    def fqn(self):
        return self.scope.fully_qualified_name(self.container, self.name)

    @property
    def resource_type(self):
        return self._resource_type

    def to_dict(self):
        return {"name": self.name}


def convert_to_resource(cls: Resource, resource_or_descriptor: Union[str, dict, Resource, ResourceName]) -> Resource:
    """Convert a resource descriptor to a resource instance"""
    if isinstance(resource_or_descriptor, str) or isinstance(resource_or_descriptor, ResourceName):
        return ResourcePointer(name=resource_or_descriptor, resource_type=cls.resource_type)
    elif isinstance(resource_or_descriptor, dict):
        # We permit two types of anonymous resource descriptors
        # 1. A dict with a single key, the name of the resource
        # Example:
        #   grant = Grant(priv='SELECT', on='table', to={'name': 'some_role'})
        # 2. A dict representing a column or column-like object
        # Example:
        #   table = Table(name='my_table', columns=[{'name': 'id', 'data_type': 'int'}])

        # if cls.__name__ == "Column":
        #     return cls(**resource_or_descriptor)
        if cls.serialize_inline:
            return cls(**resource_or_descriptor)
        else:
            return ResourcePointer(**resource_or_descriptor, resource_type=cls.resource_type)
    elif isinstance(resource_or_descriptor, cls):
        return resource_or_descriptor
    elif isinstance(resource_or_descriptor, ResourcePointer):
        if resource_or_descriptor.resource_type == cls.resource_type:
            return resource_or_descriptor
        else:
            raise ValueError(f"Unexpected resource type {resource_or_descriptor}")
    else:
        raise ValueError(f"Unexpected resource descriptor {resource_or_descriptor}")
