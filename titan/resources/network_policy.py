from dataclasses import dataclass

from ..enums import ResourceType
from ..props import (
    IdentifierListProp,
    Props,
    StringListProp,
    StringProp,
)
from ..resource_name import ResourceName
from ..scope import AccountScope
from .network_rule import NetworkRule
from .resource import NamedResource, Resource, ResourceSpec
from .role import Role


@dataclass(unsafe_hash=True)
class _NetworkPolicy(ResourceSpec):
    name: ResourceName
    allowed_network_rule_list: list[NetworkRule] = None
    blocked_network_rule_list: list[NetworkRule] = None
    allowed_ip_list: list[str] = None
    blocked_ip_list: list[str] = None
    comment: str = None
    owner: Role = "SECURITYADMIN"

    def __post_init__(self):
        super().__post_init__()
        if self.allowed_network_rule_list is not None and len(self.allowed_network_rule_list) == 0:
            raise ValueError("allowed_network_rule_list must have at least one entry")
        if self.blocked_network_rule_list is not None and len(self.blocked_network_rule_list) == 0:
            raise ValueError("blocked_network_rule_list must have at least one entry")


class NetworkPolicy(NamedResource, Resource):
    """
    Description:
        A Network Policy in Snowflake defines a set of network rules and IP addresses
        that are allowed or blocked from accessing a Snowflake account. This helps in
        managing network traffic and securing access based on network policies.

    Snowflake Docs:
        https://docs.snowflake.com/en/sql-reference/sql/create-network-policy

    Fields:
        name (string, required): The name of the network policy.
        allowed_network_rule_list (list): A list of allowed network rules.
        blocked_network_rule_list (list): A list of blocked network rules.
        allowed_ip_list (list): A list of allowed IP addresses.
        blocked_ip_list (list): A list of blocked IP addresses.
        comment (string): A comment about the network policy.
        owner (string or Role): The owner role of the network policy. Defaults to "SECURITYADMIN".

    Python:

        ```python
        network_policy = NetworkPolicy(
            name="some_network_policy",
            allowed_network_rule_list=[NetworkRule(name="rule1"), NetworkRule(name="rule2")],
            blocked_network_rule_list=[NetworkRule(name="rule3")],
            allowed_ip_list=["192.168.1.1", "192.168.1.2"],
            blocked_ip_list=["10.0.0.1"],
            comment="Example network policy"
        )
        ```

    Yaml:

        ```yaml
        network_policies:
          - name: some_network_policy
            allowed_network_rule_list:
              - rule1
              - rule2
            blocked_network_rule_list:
              - rule3
            allowed_ip_list: ["192.168.1.1", "192.168.1.2"]
            blocked_ip_list: ["10.0.0.1"]
            comment: "Example network policy"
        ```
    """

    resource_type = ResourceType.NETWORK_POLICY
    props = Props(
        allowed_network_rule_list=IdentifierListProp("allowed_network_rule_list", parens=True, eq=True),
        blocked_network_rule_list=IdentifierListProp("blocked_network_rule_list", parens=True, eq=True),
        allowed_ip_list=StringListProp("allowed_ip_list", parens=True, eq=True),
        blocked_ip_list=StringListProp("blocked_ip_list", parens=True, eq=True),
        comment=StringProp("comment"),
    )
    scope = AccountScope()
    spec = _NetworkPolicy

    def __init__(
        self,
        name: str,
        allowed_network_rule_list: list[NetworkRule] = None,
        blocked_network_rule_list: list[NetworkRule] = None,
        allowed_ip_list: list[str] = None,
        blocked_ip_list: list[str] = None,
        comment: str = None,
        owner: str = "SECURITYADMIN",
        **kwargs,
    ):
        super().__init__(name, **kwargs)
        self._data: _NetworkPolicy = _NetworkPolicy(
            name=self._name,
            allowed_network_rule_list=allowed_network_rule_list,
            blocked_network_rule_list=blocked_network_rule_list,
            allowed_ip_list=allowed_ip_list,
            blocked_ip_list=blocked_ip_list,
            comment=comment,
            owner=owner,
        )
        if self._data.allowed_network_rule_list:
            for network_rule in self._data.allowed_network_rule_list:
                self.requires(network_rule)
        if self._data.blocked_network_rule_list:
            for network_rule in self._data.blocked_network_rule_list:
                self.requires(network_rule)
