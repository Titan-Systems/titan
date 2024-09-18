import difflib
import sys
import types
from dataclasses import dataclass, field, fields
from enum import Enum
from inspect import isclass
from itertools import chain
from typing import Any, Optional, Type, TypedDict, Union, get_args, get_origin

import pyparsing as pp

from ..enums import AccountEdition, DataType, ParseableEnum, ResourceType
from ..identifiers import FQN, URN, parse_identifier, resource_label_for_type
from ..lifecycle import create_resource, drop_resource
from ..parse import _parse_create_header, _parse_props, resolve_resource_class
from ..props import Props as ResourceProps
from ..resource_name import ResourceName
from ..resource_tags import ResourceTags
from ..role_ref import RoleRef
from ..scope import (
    AccountScope,
    DatabaseScope,
    OrganizationScope,
    ResourceScope,
    SchemaScope,
    resource_can_be_contained_in,
)
from ..var import VarString, string_contains_var


class WrongContainerException(Exception):
    pass


class ResourceHasContainerException(Exception):
    pass


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


@dataclass
class LifecycleConfig:
    ignore_changes: list[str] = field(default_factory=list)
    prevent_destroy: bool = False


def _coerce_resource_field(field_value, field_type):

    # No type checking or coercion for Any
    if field_type == Any:
        return field_value

    # Recursively traverse lists and dicts
    elif get_origin(field_type) is list:
        if not isinstance(field_value, list):
            raise TypeError
        list_element_type = get_args(field_type) or (str,)
        return [_coerce_resource_field(v, field_type=list_element_type[0]) for v in field_value]

    elif get_origin(field_type) is dict:
        if not isinstance(field_value, dict):
            raise TypeError
        dict_types = get_args(field_type)
        if len(dict_types) < 2:
            raise RuntimeError(f"Unexpected field type {field_type}")
        return {k: _coerce_resource_field(v, field_type=dict_types[1]) for k, v in field_value.items()}

    elif field_type is RoleRef:
        return convert_role_ref(field_value)

    # Check for field_value's type in a Union
    elif get_origin(field_type) == Union:
        union_types = get_args(field_type)
        for union_type in union_types:
            expected_type = get_origin(union_type) or union_type
            if isinstance(field_value, expected_type):
                return _coerce_resource_field(field_value, field_type=expected_type)
        raise RuntimeError(f"Unexpected field type {field_type}")

    elif not isclass(field_type):
        raise RuntimeError(f"Unexpected field type {field_type}")

    # Coerce enums
    elif issubclass(field_type, ParseableEnum):
        try:
            new_value = field_type(field_value)
        except ValueError:
            raise TypeError
        return new_value

    # Coerce args
    elif field_type is Arg:
        arg_dict = {
            "name": field_value["name"].upper(),
            "data_type": DataType(field_value["data_type"]),
        }
        if "default" in field_value:
            arg_dict["default"] = field_value["default"]
        return arg_dict

    # Coerce returns
    elif field_type is Returns:
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
    elif field_type is ResourceName:
        return field_value if isinstance(field_value, VarString) else ResourceName(field_value)
    elif field_type is ResourceTags:
        return ResourceTags(field_value)
    elif field_type is str:
        if isinstance(field_value, str) and string_contains_var(field_value):
            return VarString(field_value)
        elif isinstance(field_value, VarString):
            return field_value
        elif not isinstance(field_value, str):
            raise TypeError
        else:
            return field_value
    else:
        # Typecheck all other field types (str, int, etc.)
        if not isinstance(field_value, field_type):
            raise TypeError
        return field_value


@dataclass
class ResourceSpecMetadata:
    fetchable: bool = True
    triggers_replacement: bool = False
    forces_add: bool = False
    ignore_changes: bool = False
    known_after_apply: bool = False


@dataclass
class ResourceSpec:
    def __post_init__(self):
        for f in fields(self):
            field_value = getattr(self, f.name)
            if field_value is None:
                continue
            else:
                try:
                    new_value = _coerce_resource_field(field_value, f.type)
                    setattr(self, f.name, new_value)
                except TypeError as err:
                    human_readable_classname = self.__class__.__name__[1:]
                    if issubclass(f.type, Enum):
                        raise TypeError(
                            f"Expected {human_readable_classname}.{f.name} to be one of ({', '.join(f.type.__members__.keys())}), got {repr(field_value)} instead"
                        ) from err
                    else:
                        raise TypeError(
                            f"Expected {human_readable_classname}.{f.name} to be {f.type}, got {repr(field_value)} instead"
                        ) from err

    @classmethod
    def get_metadata(cls, field_name: str) -> ResourceSpecMetadata:
        for f in fields(cls):
            if f.name == field_name:
                return ResourceSpecMetadata(**f.metadata)
        raise ValueError(f"Field {field_name} not found in {cls.__name__}")


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

    def __init__(
        self,
        implicit: bool = False,
        lifecycle: dict = None,
        **kwargs,
    ):
        super().__init__()
        self._data: ResourceSpec = None
        self._container: "ResourceContainer" = None
        self._finalized = False
        self.lifecycle = LifecycleConfig(**lifecycle) if lifecycle else LifecycleConfig()
        self.implicit = implicit
        self.refs: set[Resource] = set()

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
                    field_names = [f.name for f in fields(self.spec)]
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
            resource_type = resolve_resource_class(sql)
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

    def __repr__(self):  # pragma: no cover
        if not hasattr(self, "_data"):
            return f"{self.__class__.__name__}(<uninitialized>)"
        name = getattr(self._data, "name", "<noname>")
        implicit = "~" if self.implicit else ""
        return f"{self.__class__.__name__}({implicit}{name})"

    def __eq__(self, other):
        if not isinstance(other, Resource):
            return False
        return self._data == other._data

    def __hash__(self):
        return hash(URN.from_resource(self, ""))

    def to_dict(self):
        serialized: dict[str, Any] = {}
        if self.implicit:
            serialized["_implicit"] = True

        def _serialize(field, value):
            if field.name == "owner":
                return str(value.fqn)
            elif isinstance(value, ResourcePointer):
                return str(value.fqn)
            elif isinstance(value, Resource):
                if getattr(value, "serialize_inline", False):
                    return value.to_dict()
                elif isinstance(value, NamedResource):
                    return str(value.fqn)
                else:
                    raise Exception(f"Cannot serialize {value}")
            elif isinstance(value, ParseableEnum):
                return str(value)
            elif isinstance(value, list):
                return [_serialize(field, v) for v in value]
            elif isinstance(value, dict):
                return {k: _serialize(field, v) for k, v in value.items()}
            elif isinstance(value, ResourceName):
                return str(value)
            elif isinstance(value, ResourceTags):
                return value.tags
            else:
                return value

        for f in fields(self._data):
            value = getattr(self._data, f.name)
            serialized[f.name] = _serialize(f, value)

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
        if self._finalized:
            raise RuntimeError("Cannot modify a finalized resource")
        if isinstance(resource, (Resource, ResourcePointer)):
            self.refs.add(resource)

    def requires(self, *resources: "Resource"):
        if isinstance(resources[0], list):
            resources = resources[0]
        for resource in resources:
            self._requires(resource)

    def _register_scope(self, database=None, schema=None):
        if isinstance(database, str):
            database = ResourcePointer(name=database, resource_type=ResourceType.DATABASE)

        if isinstance(schema, str):
            schema = ResourcePointer(name=schema, resource_type=ResourceType.SCHEMA)
            if database is not None:
                database.add(schema)

        if isinstance(self.scope, DatabaseScope):
            if schema is not None:
                raise RuntimeError(f"Unexpected kwarg schema {schema} for resource {self}")
            if database is not None:
                database.add(self)

        elif isinstance(self.scope, SchemaScope):
            if schema is not None:
                schema.add(self)
                if database is not None:
                    if schema.container is None:
                        database.add(schema)
                    elif schema.container != database:
                        raise ResourceHasContainerException(f"Schema {schema} does not belong to database {database}")

            elif database is not None:
                database.find(name="PUBLIC", resource_type=ResourceType.SCHEMA).add(self)

    def _resolve_vars(self, vars: dict):

        def _render_vars(field_value):
            if isinstance(field_value, VarString):
                return field_value.to_string(vars)
            elif isinstance(field_value, list):
                return [_render_vars(v) for v in field_value]
            elif isinstance(field_value, dict):
                return {k: _render_vars(v) for k, v in field_value.items()}
            elif isinstance(field_value, Resource) and not isinstance(field_value, ResourcePointer):
                field_value._resolve_vars(vars)
                return field_value
            else:
                return field_value

        if self._data:
            for f in fields(self._data):
                field_value = getattr(self._data, f.name)
                new_value = _render_vars(field_value)
                setattr(self._data, f.name, new_value)

        if isinstance(self, NamedResource) and isinstance(self._name, VarString):
            self._name = ResourceName(self._name.to_string(vars))

    def to_pointer(self):
        return ResourcePointer(
            name=str(self.fqn),
            resource_type=self.resource_type,
        )

    @property
    def container(self):
        return self._container

    @property
    def urn(self):
        return URN.from_resource(self, account_locator="")

    @property
    def fqn(self) -> FQN:
        raise NotImplementedError("Subclasses must implement fqn")


class ResourceContainer:
    def __init__(self):
        self._items: dict[ResourceType, list[Resource]] = {}

    def __contains__(self, item: Resource):
        return item in self._items.get(item.resource_type, [])

    def add(self, *items: Resource):
        if isinstance(items[0], list):
            items = items[0]
        for item in items:
            if not resource_can_be_contained_in(item, self):
                raise WrongContainerException(f"{item} cannot be added to {self}")
            if item.container is not None and not isinstance(item.container, ResourcePointer):
                raise ResourceHasContainerException(f"{item} already belongs to a container")
            item._container = self
            item.requires(self)
            if item.resource_type not in self._items:
                self._items[item.resource_type] = []
            self._items[item.resource_type].append(item)

    def items(self, resource_type: Optional[ResourceType] = None) -> list[Resource]:
        if resource_type:
            return self._items.get(resource_type, [])
        else:
            return list(chain.from_iterable(self._items.values()))

    def find(self, resource_type: ResourceType, name: str) -> Resource:
        for resource in self.items(resource_type):
            if isinstance(resource, ResourcePointer) and resource.name == name:
                return resource
            elif (
                isinstance(resource, Resource)
                and resource._data is not None
                and getattr(resource._data, "name", None) == name
            ):
                return resource
        raise KeyError(f"Resource {resource_type} {name} not found")

    def remove(self, resource: Resource):
        if resource.resource_type in self._items:
            for index, item in enumerate(self._items[resource.resource_type]):
                if item is resource:
                    self._items[resource.resource_type].pop(index)
                    resource._container = None
            resource.refs.remove(self)


class NamedResource:
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

    def __init__(self, name: Union[str, ResourceName, VarString], **kwargs):
        if not isinstance(name, (str, ResourceName, VarString)):
            raise TypeError(f"Expected str or ResourceName for name, got {name} ({type(name).__name__}) instead")

        if isinstance(name, (ResourceName, VarString)):
            self._name = name
            super().__init__(**kwargs)
            return

        if string_contains_var(name):
            self._name = VarString(name)
            super().__init__(**kwargs)
            return

        if isinstance(self.scope, AccountScope):
            self._name = ResourceName(name)
            super().__init__(**kwargs)
            return

        try:
            identifier = parse_identifier(name, is_db_scoped=isinstance(self.scope, DatabaseScope))
        except pp.ParseException:
            # Allow identifiers that should be quoted, so long as they aren't insane
            if "." in name:
                raise ValueError(f"Resource name not supported {name}")
            identifier = parse_identifier(f'"{name}"', is_db_scoped=isinstance(self.scope, DatabaseScope))

        self._name = ResourceName(identifier["name"])
        if "database" in identifier and "database" in kwargs:
            raise ValueError("Multiple database names found")
        if "schema" in identifier and "schema" in kwargs:
            raise ValueError("Multiple schema names found")

        super().__init__(
            database=kwargs.pop("database", identifier.get("database")),
            schema=kwargs.pop("schema", identifier.get("schema")),
            **kwargs,
        )

    @property
    def name(self):
        return self._name

    @property
    def fqn(self) -> FQN:
        return self.scope.fully_qualified_name(self.container, self.name)


class ResourcePointer(NamedResource, Resource, ResourceContainer):
    def __init__(self, name: str, resource_type: ResourceType):
        self._resource_type: ResourceType = resource_type
        self.scope = RESOURCE_SCOPES[resource_type]
        super().__init__(name)

        # Don't want to do this for all implicit resources but making an exception for PUBLIC schema
        # If this points to a database, assume it includes a PUBLIC schema
        if self._resource_type == ResourceType.DATABASE and self._name != "SNOWFLAKE":
            self.add(ResourcePointer(name="PUBLIC", resource_type=ResourceType.SCHEMA))
            # self.add(ResourcePointer(name="INFORMATION_SCHEMA", resource_type=ResourceType.SCHEMA))

    def __repr__(self):  # pragma: no cover
        resource_type = getattr(self, "resource_type", None)
        resource_label = resource_label_for_type(resource_type).title() if resource_type else "resource"
        name = getattr(self, "name", None)
        return f"{resource_label}(→{name})"

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
        return {
            "_pointer": True,
            "name": self.name,
        }


def convert_to_resource(cls: Resource, resource_or_descriptor: Union[str, dict, Resource, ResourceName]) -> Resource:
    """
    This function helps provide flexibility to users on how Resource fields are set.
    The most common use case is to allow a user to pass in a string of the name of a resource without
    the need to specify the resource type.

    For example:
    >>> schema = Schema(name="my_schema", owner="some_role")

    Another use case is to allow a user to pass in simple Python data structures for things like table columns.
    >>> table = Table(name='my_table', columns=[{'name': 'id', 'data_type': 'int'}])
    """
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
        if cls.serialize_inline:
            return cls(**resource_or_descriptor)
        else:
            # This isn't intended to be used by users. This handles the case when a
            # ResourcePointer is deserialized and re-serialized.
            return ResourcePointer(**resource_or_descriptor, resource_type=cls.resource_type)
    elif isinstance(resource_or_descriptor, cls):
        # We are expecting an instance of type cls. If it's a match, return it.
        return resource_or_descriptor
    elif isinstance(resource_or_descriptor, ResourcePointer):
        if resource_or_descriptor.resource_type == cls.resource_type:
            return resource_or_descriptor
        else:
            raise TypeError
    else:
        raise TypeError


def convert_role_ref(role_ref: RoleRef) -> Resource:
    if role_ref.__class__.__name__ == "Role":
        return role_ref
    elif role_ref.__class__.__name__ == "DatabaseRole":
        return role_ref
    elif isinstance(role_ref, ResourcePointer) and role_ref.resource_type in (
        ResourceType.DATABASE_ROLE,
        ResourceType.ROLE,
    ):
        return role_ref
    elif isinstance(role_ref, str):
        return ResourcePointer(name=role_ref, resource_type=infer_role_type_from_name(role_ref))
    else:
        raise TypeError


def infer_role_type_from_name(name: str) -> ResourceType:
    if name == "":
        return ResourceType.ROLE
    identifier = parse_identifier(name, is_db_scoped=True)
    if "database" in identifier:
        return ResourceType.DATABASE_ROLE
    else:
        return ResourceType.ROLE
