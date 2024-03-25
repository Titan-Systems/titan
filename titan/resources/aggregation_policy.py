from dataclasses import dataclass

from .resource import Resource, ResourceSpec
from ..enums import ParseableEnum, ResourceType
from ..scope import SchemaScope

from ..props import (
    EnumProp,
    Props,
    StringProp,
    StringListProp,
)


class AggregationPolicyMode(ParseableEnum):
    APPEND = "APPEND"
    OVERWRITE = "OVERWRITE"


@dataclass(unsafe_hash=True)
class _AggregationPolicy(ResourceSpec):
    name: str
    target: str
    mode: AggregationPolicyMode = AggregationPolicyMode.APPEND
    comment: str = None
    owner: str = "SYSADMIN"

    def __post_init__(self):
        super().__post_init__()
        if not self.target:
            raise ValueError("TARGET is required for an Aggregation Policy.")


class AggregationPolicy(Resource):
    """
    An Aggregation Policy defines how data is aggregated within a Snowflake account.

    CREATE [ OR REPLACE ] AGGREGATION POLICY <name>
       TARGET = '<target>'
       MODE = { APPEND | OVERWRITE }
       [ COMMENT = '<string_literal>' ]
    """

    resource_type = ResourceType.AGGREGATION_POLICY
    props = Props(
        target=StringProp("target"),
        mode=EnumProp("mode", AggregationPolicyMode),
        comment=StringProp("comment"),
    )
    scope = SchemaScope()
    spec = _AggregationPolicy

    def __init__(
        self,
        name: str,
        target: str,
        mode: AggregationPolicyMode = AggregationPolicyMode.APPEND,
        comment: str = None,
        owner: str = "SYSADMIN",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._data: _AggregationPolicy = _AggregationPolicy(
            name=name,
            target=target,
            mode=mode,
            comment=comment,
            owner=owner,
        )
