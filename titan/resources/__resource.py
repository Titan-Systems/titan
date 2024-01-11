from dataclasses import asdict, dataclass, fields
from typing import _GenericAlias, Type

from ..enums import ParseableEnum, ResourceType
from ..identifiers import FQN, URN
from ..lifecycle import create_resource, drop_resource
from ..props import Props as ResourceProps
from ..parse import ParseException, _parse_create_header, _parse_props
from ..scope import ResourceScope


@dataclass
class ResourceSpec:
    def __post_init__(self):
        for field in fields(self):
            field_value = getattr(self, field.name)

            if field_value is None:
                continue
            if isinstance(field.type, _GenericAlias):
                continue
            elif issubclass(field.type, ParseableEnum):
                # Coerce enums
                setattr(self, field.name, field.type(field_value))
            elif issubclass(field.type, Resource):
                # Coerce resources
                if isinstance(field_value, str):
                    setattr(self, field.name, field.type(name=field_value, stub=True))
                elif isinstance(field_value, dict):
                    setattr(self, field.name, field.type(**field_value, stub=True))


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
    requires: set = set()

    def __init__(self, implicit: bool = False, stub: bool = False, **scope_kwargs):
        self._data = None
        self.implicit = implicit
        self.stub = stub
        self.refs = []
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
            for key, value in asdict(self._data).items():
                skip_field = (value is None) or (value == defaults[key])
                skip_field = skip_field and (key != "owner")
                if skip_field:
                    continue
                if isinstance(value, Resource):
                    serialized[key] = value.to_dict(packed=True)
                elif isinstance(value, ParseableEnum):
                    serialized[key] = str(value)
                else:
                    serialized[key] = value
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

    @property
    def fqn(self):
        return self.scope.fully_qualified_name(self._data.name)

    @property
    def urn(self):
        return URN.from_resource(self, account_locator="")

    # @classmethod
    # def from_dict(cls, urn, data):
    #     return cls(**data)
