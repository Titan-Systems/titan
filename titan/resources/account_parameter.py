from dataclasses import dataclass
from typing import Any

from ..enums import ResourceType
from ..parse import parse_alter_account_parameter
from ..props import Props
from ..resource_name import ResourceName
from ..scope import AccountScope
from .resource import NamedResource, Resource, ResourceSpec


@dataclass(unsafe_hash=True)
class _AccountParameter(ResourceSpec):
    name: ResourceName
    value: Any


class AccountParameter(NamedResource, Resource):
    resource_type = ResourceType.ACCOUNT_PARAMETER
    props = Props()
    scope = AccountScope()
    spec = _AccountParameter

    def __init__(self, name: str, value: Any, **kwargs):
        super().__init__(name=name, **kwargs)
        self._data: _AccountParameter = _AccountParameter(
            name=self._name,
            value=value,
        )

    @classmethod
    def from_sql(cls, sql):
        props = parse_alter_account_parameter(sql)
        return cls(**props)
