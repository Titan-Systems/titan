from dataclasses import asdict, dataclass, fields
from typing import _GenericAlias, TypedDict, Type, get_args, get_type_hints
from inspect import isclass

from ..enums import DataType, ParseableEnum, ResourceType
from ..identifiers import FQN, URN
from ..lifecycle import create_resource, drop_resource
from ..props import Props as ResourceProps
from ..parse import ParseException, _parse_create_header, _parse_props
from ..scope import ResourceScope


class Arg(TypedDict):
    name: str
    data_type: DataType


@dataclass
class ResourceSpec:
    def __post_init__(self):
        for field in fields(self):
            field_value = getattr(self, field.name)

            def _coerce(field_value, field_type=None):
                if field_type is None:
                    field_type = field.type

                # Recursively traverse lists and dicts
                if issubclass(field_type, list):
                    if not isinstance(field_value, list):
                        raise Exception
                    list_element_type = get_args(field_type) or (str,)
                    return [_coerce(v, field_type=list_element_type[0]) for v in field_value]
                elif issubclass(field_type, dict):
                    if not isinstance(field_value, dict):
                        raise Exception
                    if field_type == Arg:
                        type_map = get_type_hints(field_type)
                        return {k: _coerce(v, field_type=type_map[k]) for k, v in field_value.items()}
                    else:
                        dict_types = get_args(field_type)
                        if len(dict_types) < 2:
                            raise RuntimeError(f"Unexpected field type {field_type}")
                        return {k: _coerce(v, field_type=dict_types[1]) for k, v in field_value.items()}

                if not isclass(field_type):
                    raise RuntimeError(f"Unexpected field type {field_type}")

                # Coerce enums
                if issubclass(field_type, ParseableEnum):
                    return field_type(field_value)

                # Coerce resources
                elif issubclass(field_type, Resource):
                    if isinstance(field_value, str):
                        return field_type(name=field_value, stub=True)
                    elif isinstance(field_value, dict):
                        return field_type(**field_value, stub=True)
                else:
                    return field_value

            if field_value is None:
                continue
            elif isinstance(field.type, _GenericAlias):
                continue
            else:
                setattr(self, field.name, _coerce(field_value))


class _Resource(type):
    __types = {}

    def __new__(cls, name, bases, attrs):
        cls_ = super().__new__(cls, name, bases, attrs)
        try:
            if cls_.resource_type not in cls._Resource__types:
                cls.__types[cls_.resource_type] = []
            cls.__types[cls_.resource_type].append(cls_)
        except AttributeError:
            pass
        return cls_


class Resource(metaclass=_Resource):
    props: ResourceProps
    resource_type: ResourceType
    scope: ResourceScope
    spec: Type[ResourceSpec]
    refs: set
    _container: "ResourceContainer"

    def __init__(self, implicit: bool = False, stub: bool = False, **scope_kwargs):
        super().__init__()
        self._data = None
        self.implicit = implicit
        self.stub = stub
        self.refs = set()
        self.scope.register_scope(**scope_kwargs)

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
        except ParseException as err:
            raise ParseException(f"Error parsing {resource_cls.__name__} props {identifier}") from err

    @classmethod
    def props_for_resource_type(cls, resource_type: ResourceType):
        resource_types = cls.__types[resource_type]
        if len(resource_types) > 1:
            raise NotImplementedError
        return resource_types[0].props

    def to_dict(self, packed=False):
        defaults = {f.name: f.default for f in fields(self.spec)}
        if packed:
            serialized = {}

            def _serialize(value):
                if isinstance(value, Resource):
                    return value.to_dict(packed=True)
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
                    continue
                serialized[key] = _serialize(value)

            return serialized
        else:
            return asdict(self._data)

    def create_sql(self, **kwargs):
        return create_resource(
            self.urn,
            self.to_dict(packed=True),
            self.props,
            **kwargs,
        )

    def drop_sql(self, if_exists: bool = False):
        return drop_resource(self.urn, if_exists=if_exists)

    def requires(self, *resources):
        self.refs.update(resources)

    @property
    def container(self):
        return self._container

    @property
    def fqn(self):
        return self.scope.fully_qualified_name(self._data.name)

    @property
    def urn(self):
        return URN.from_resource(self, account_locator="")

    # @classmethod
    # def from_dict(cls, urn, data):
    #     return cls(**data)


class ResourceContainer:
    def __init__(self):
        self._items = []

    def add(self, *items: Resource):
        if isinstance(items[0], list):
            items = items[0]
        for item in items:
            if item.container:
                raise RuntimeError(f"{item} already belongs to a container")
            item._container = self
            item.requires(self)
            self._items.append(item)

    def items(self) -> list[Resource]:
        return self._items
