from dataclasses import dataclass

from .resource import Resource, ResourceSpec, ResourceNameTrait
from .role import Role
from ..enums import ResourceType
from ..resource_name import ResourceName
from ..scope import SchemaScope
from ..enums import AccountEdition

from ..props import (
    EnumProp,
    Props,
    StringProp,
)


@dataclass(unsafe_hash=True)
class _AggregationPolicy(ResourceSpec):
    name: ResourceName
    body: str
    comment: str = None
    owner: Role = "SYSADMIN"


# TODO:
# Aggregation policies have a weird construction and may require new prop types to handle
# CREATE [ OR REPLACE ] AGGREGATION POLICY [ IF NOT EXISTS ] <name>
#   AS () RETURNS AGGREGATION_CONSTRAINT -> <body>
#   [ COMMENT = '<string_literal>' ]


class AggregationPolicy(ResourceNameTrait, Resource):
    edition = {AccountEdition.ENTERPRISE, AccountEdition.BUSINESS_CRITICAL}
    resource_type = ResourceType.AGGREGATION_POLICY
    props = Props(
        _start_token="AS ()",
        body=StringProp("body"),
        comment=StringProp("comment"),
    )
    scope = SchemaScope()
    spec = _AggregationPolicy

    def __init__(
        self,
        name: str,
        body: str,
        comment: str = None,
        owner: str = "SYSADMIN",
        **kwargs,
    ):
        super().__init__(name, **kwargs)
        self._data: _AggregationPolicy = _AggregationPolicy(
            name=self._name,
            body=body,
            comment=comment,
            owner=owner,
        )
