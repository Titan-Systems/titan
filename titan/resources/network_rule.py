from dataclasses import dataclass

from ..enums import ParseableEnum, ResourceType
from ..props import (
    EnumProp,
    Props,
    StringListProp,
    StringProp,
)
from ..resource_name import ResourceName
from ..role_ref import RoleRef
from ..scope import SchemaScope
from .resource import NamedResource, Resource, ResourceSpec


class NetworkIdentifierType(ParseableEnum):
    IPV4 = "IPV4"
    AWSVPCEID = "AWSVPCEID"
    AZURELINKID = "AZURELINKID"
    HOST_PORT = "HOST_PORT"


class NetworkRuleMode(ParseableEnum):
    INGRESS = "INGRESS"
    INTERNAL_STAGE = "INTERNAL_STAGE"
    EGRESS = "EGRESS"


@dataclass(unsafe_hash=True)
class _NetworkRule(ResourceSpec):
    name: ResourceName
    type: NetworkIdentifierType
    value_list: list[str] = None
    mode: NetworkRuleMode = NetworkRuleMode.INGRESS
    comment: str = None
    owner: RoleRef = "SYSADMIN"

    def __post_init__(self):
        super().__post_init__()
        if self.type == NetworkIdentifierType.HOST_PORT and self.mode != NetworkRuleMode.EGRESS:
            raise ValueError("When TYPE is HOST_PORT, MODE must be set to EGRESS.")

        if self.value_list is not None and len(self.value_list) == 0:
            raise ValueError("value_list must have at least one entry")


class NetworkRule(NamedResource, Resource):
    """
    Description:
        A Network Rule in Snowflake defines a set of network addresses, such as IP addresses or hostnames,
        that can be allowed or denied access to a Snowflake account. This helps in managing network traffic
        and securing access based on network policies.

    Snowflake Docs:
        https://docs.snowflake.com/en/sql-reference/sql/create-network-rule

    Fields:
        name (string, required): The name of the network rule.
        type (string or NetworkIdentifierType, required): The type of network identifier. Defaults to IPV4.
        value_list (list): A list of values associated with the network rule.
        mode (string or NetworkRuleMode): The mode of the network rule. Defaults to INGRESS.
        comment (string): A comment about the network rule.
        owner (string or Role): The owner role of the network rule. Defaults to "SYSADMIN".

    Python:

        ```python
        network_rule = NetworkRule(
            name="some_network_rule",
            type="IPV4",
            value_list=["192.168.1.1", "192.168.1.2"],
            mode="INGRESS",
            comment="Example network rule"
        )
        ```

    Yaml:

        ```yaml
        network_rules:
          - name: some_network_rule
            type: IPV4
            value_list: ["192.168.1.1", "192.168.1.2"]
            mode: INGRESS
            comment: "Example network rule"
        ```
    """

    resource_type = ResourceType.NETWORK_RULE
    props = Props(
        type=EnumProp("type", NetworkIdentifierType),
        value_list=StringListProp("value_list", parens=True, eq=True),
        mode=EnumProp("mode", NetworkRuleMode),
        comment=StringProp("comment"),
    )
    scope = SchemaScope()
    spec = _NetworkRule

    def __init__(
        self,
        name: str,
        type: NetworkIdentifierType = NetworkIdentifierType.IPV4,
        value_list: list[str] = None,
        mode: NetworkRuleMode = NetworkRuleMode.INGRESS,
        comment: str = None,
        owner: str = "SYSADMIN",
        **kwargs,
    ):
        super().__init__(name, **kwargs)
        self._data: _NetworkRule = _NetworkRule(
            name=self._name,
            type=type,
            value_list=value_list,
            mode=mode,
            comment=comment,
            owner=owner,
        )
