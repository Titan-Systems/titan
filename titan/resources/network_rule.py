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


class NetworkIdentifierType(ParseableEnum):
    IPV4 = "IPV4"
    AWSVPCEID = "AWSVPCEID"
    AZURELINKID = "AZURELINKID"
    HOST_PORT = "HOST_PORT"


class NetworkRuleMode(ParseableEnum):
    INGRESS = "INGRESS"
    INTERNAL_STAGE = "INTERNAL_STAGE"
    EGRESS = "EGRESS"


@dataclass
class _NetworkRule(ResourceSpec):
    name: str
    type: NetworkIdentifierType
    value_list: list[str]
    mode: NetworkRuleMode = NetworkRuleMode.INGRESS
    comment: str = None
    owner: str = "SYSADMIN"

    def __post_init__(self):
        super().__post_init__()
        if self.type == NetworkIdentifierType.HOST_PORT and self.mode != NetworkRuleMode.EGRESS:
            raise ValueError("When TYPE is HOST_PORT, MODE must be set to EGRESS.")


class NetworkRule(Resource):
    """
    A Network Rule defines a set of network addresses (eg. IP addresses or hostnames) that can be allowed or
    denied access from a Snowflake account.

    CREATE [ OR REPLACE ] NETWORK RULE <name>
       TYPE = { IPV4 | AWSVPCEID | AZURELINKID | HOST_PORT }
       VALUE_LIST = ( '<value>' [, '<value>', ... ] )
       MODE = { INGRESS | INTERNAL_STAGE | EGRESS }
       [ COMMENT = '<string_literal>' ]
    """

    resource_type = ResourceType.NETWORK_RULE
    props = Props(
        type=EnumProp("type", NetworkIdentifierType),
        value_list=StringListProp("value_list", parens=True),
        mode=EnumProp("mode", NetworkRuleMode),
        comment=StringProp("comment"),
    )
    scope = SchemaScope()
    spec = _NetworkRule

    def __init__(
        self,
        name: str,
        type: NetworkIdentifierType = NetworkIdentifierType.IPV4,
        value_list: list[str] = [],
        mode: NetworkRuleMode = NetworkRuleMode.INGRESS,
        comment: str = None,
        owner: str = "SYSADMIN",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._data: _NetworkRule = _NetworkRule(
            name=name,
            type=type,
            value_list=value_list,
            mode=mode,
            comment=comment,
            owner=owner,
        )
