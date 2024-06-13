from dataclasses import dataclass

from .resource import Resource, ResourceSpec
from .role import Role
from ..enums import ResourceType
from ..scope import SchemaScope

from ..props import (
    EnumProp,
    Props,
    StringProp,
)


@dataclass(unsafe_hash=True)
class _AggregationPolicy(ResourceSpec):
    name: str
    body: str
    comment: str = None
    owner: Role = "SYSADMIN"


# TODO:
# Aggregation policies have a weird construction and may require new prop types to handle
# CREATE [ OR REPLACE ] AGGREGATION POLICY [ IF NOT EXISTS ] <name>
#   AS () RETURNS AGGREGATION_CONSTRAINT -> <body>
#   [ COMMENT = '<string_literal>' ]


class AggregationPolicy(Resource):
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
        super().__init__(**kwargs)
        self._data: _AggregationPolicy = _AggregationPolicy(
            name=name,
            body=body,
            comment=comment,
            owner=owner,
        )
