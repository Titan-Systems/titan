from dataclasses import dataclass

from .resource import Resource, ResourceSpec, NamedResource
from ..resource_name import ResourceName
from .network_rule import NetworkRule
from .role import Role
from ..enums import ResourceType
from ..scope import AccountScope

from ..props import BoolProp, Props, StringProp, IdentifierListProp


@dataclass(unsafe_hash=True)
class _ExternalAccessIntegration(ResourceSpec):
    name: ResourceName
    allowed_network_rules: list[NetworkRule]
    allowed_api_authentication_integrations: list[str] = None
    # This is a special case where this resource relies on another polymorphic resource
    allowed_authentication_secrets: list[str] = None
    enabled: bool = True
    comment: str = None
    owner: Role = "ACCOUNTADMIN"

    def __post_init__(self):
        super().__post_init__()
        if self.allowed_network_rules is not None and len(self.allowed_network_rules) < 1:
            raise ValueError("allowed_network_rules must have at least one element")

        if self.allowed_api_authentication_integrations and len(self.allowed_api_authentication_integrations) < 1:
            raise ValueError("allowed_api_authentication_integrations must have at least one element if specified")

        if self.allowed_authentication_secrets:
            if len(self.allowed_authentication_secrets) < 1:
                raise ValueError("allowed_authentication_secrets must have at least one element if specified")

            if "any" in self.allowed_authentication_secrets and len(self.allowed_authentication_secrets) > 1:
                raise ValueError("allowed_authentication_secrets must not contain 'any' if there are other secrets")

            if "none" in self.allowed_authentication_secrets and len(self.allowed_authentication_secrets) > 1:
                raise ValueError("allowed_authentication_secrets must not contain 'none' if there are other secrets")


class ExternalAccessIntegration(NamedResource, Resource):
    """
    Description:
        External Access Integrations enable code within functions and stored procedures to utilize secrets and establish connections with external networks. This resource configures the rules and secrets that can be accessed by such code.

    Snowflake Docs:
        https://docs.snowflake.com/en/sql-reference/sql/create-external-access-integration

    Fields:
        name (string, required): The name of the external access integration.
        allowed_network_rules (list, required): [NetworkRules](network_rule.md) that are allowed for this integration.
        allowed_api_authentication_integrations (list): API authentication integrations that are allowed.
        allowed_authentication_secrets (list): Authentication secrets that are allowed.
        enabled (bool): Specifies if the integration is enabled. Defaults to True.
        comment (string): An optional comment about the integration.
        owner (string or Role): The owner role of the external access integration. Defaults to "ACCOUNTADMIN".

    Python:

        ```python
        external_access_integration = ExternalAccessIntegration(
            name="some_external_access_integration",
            allowed_network_rules=["rule1", "rule2"],
            enabled=True
        )
        ```

    Yaml:

        ```yaml
        external_access_integrations:
          - name: some_external_access_integration
            allowed_network_rules:
              - rule1
              - rule2
            enabled: true
        ```
    """

    resource_type = ResourceType.EXTERNAL_ACCESS_INTEGRATION
    props = Props(
        allowed_network_rules=IdentifierListProp("allowed_network_rules", eq=True, parens=True),
        allowed_api_authentication_integrations=IdentifierListProp(
            "allowed_api_authentication_integrations", eq=True, parens=True
        ),
        allowed_authentication_secrets=IdentifierListProp("allowed_authentication_secrets", eq=True, parens=True),
        enabled=BoolProp("enabled"),
        comment=StringProp("comment"),
    )
    scope = AccountScope()
    spec = _ExternalAccessIntegration

    def __init__(
        self,
        name: str,
        allowed_network_rules: list[NetworkRule] = None,
        allowed_api_authentication_integrations: list[str] = None,
        allowed_authentication_secrets: list[str] = None,
        enabled: bool = True,
        comment: str = None,
        owner: str = "ACCOUNTADMIN",
        **kwargs,
    ):
        super().__init__(name, **kwargs)
        self._data: _ExternalAccessIntegration = _ExternalAccessIntegration(
            name=self._name,
            allowed_network_rules=allowed_network_rules or [],
            allowed_api_authentication_integrations=allowed_api_authentication_integrations,
            allowed_authentication_secrets=allowed_authentication_secrets,
            enabled=enabled,
            comment=comment,
            owner=owner,
        )
        self.requires(self._data.allowed_network_rules)
        self.requires(self._data.allowed_authentication_secrets)
