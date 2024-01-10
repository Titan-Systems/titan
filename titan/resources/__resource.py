from abc import ABC
from dataclasses import asdict, dataclass, fields
from typing import _GenericAlias, Type

from ..enums import ParseableEnum, ResourceType
from ..identifiers import FQN, URN
from ..lifecycle import create_resource, drop_resource
from ..props import Props as ResourceProps
from ..parse import ParseException, _resolve_resource_class, _parse_create_header, _parse_props


class ResourceScope(ABC):
    def fully_qualified_name(self, resource: "Resource"):
        raise NotImplementedError


class AccountScope(ResourceScope):
    def fully_qualified_name(self, resource: "Resource"):
        return FQN(name=resource._data.name.upper())


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


class Resource:
    props: ResourceProps
    resource_type: ResourceType
    scope: ResourceScope
    spec: Type[ResourceSpec]

    def __init__(self, implicit: bool = False, stub: bool = False):
        self._data = None
        self.implicit = implicit
        self.stub = stub

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

    def to_dict(self, packed=False):
        # fields(self.spec)
        # TODO: remove default values
        if packed:
            return {k: v for k, v in asdict(self._data).items() if v is not None}
        else:
            return asdict(self._data)

    def create_sql(self, **kwargs):
        data = self.to_dict(packed=True)
        return create_resource(self.urn, data, **kwargs)

    def drop_sql(self, if_exists: bool = False):
        return drop_resource(self.urn, if_exists=if_exists)

    @property
    def fqn(self):
        return self.scope.fully_qualified_name(self)

    @property
    def urn(self):
        return URN.from_resource(self, account_locator="")

    # @classmethod
    # def from_dict(cls, urn, data):
    #     return cls(**data)
