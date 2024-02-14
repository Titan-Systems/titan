from dataclasses import dataclass

from .resource import Resource, ResourceSpec
from .network_rule import NetworkRule
from .secret import Secret
from ..enums import ResourceType
from ..scope import AccountScope

from ..props import BoolProp, Props, StringProp, IdentifierListProp


@dataclass(unsafe_hash=True)
class _ExternalAccessIntegration(ResourceSpec):
    name: str
    allowed_network_rules: list[NetworkRule]
    allowed_api_authentication_integrations: list[str] = None
    allowed_authentication_secrets: list[Secret] = None
    enabled: bool = True
    comment: str = None
    owner: str = "ACCOUNTADMIN"

    def __post_init__(self):
        super().__post_init__()
        if self.allowed_network_rules is not None and len(self.allowed_network_rules) < 1:
            raise ValueError("allowed_network_rules must have at least one element if not None")
        if (
            self.allowed_api_authentication_integrations is not None
            and len(self.allowed_api_authentication_integrations) < 1
        ):
            raise ValueError("allowed_api_authentication_integrations must have at least one element if not None")
        if self.allowed_authentication_secrets is not None and len(self.allowed_authentication_secrets) < 1:
            raise ValueError("allowed_authentication_secrets must have at least one element if not None")


class ExternalAccessIntegration(Resource):
    """
    External Access Integrations allow code in functions and stored procedures to use secrets and to connect with external networks.

    CREATE [ OR REPLACE ] EXTERNAL ACCESS INTEGRATION <name>
      ALLOWED_NETWORK_RULES = ( <rule_name_1> [, <rule_name_2>, ... ] )
      [ ALLOWED_API_AUTHENTICATION_INTEGRATIONS = ( <integration_name_1> [, <integration_name_2>, ... ] ) ]
      [ ALLOWED_AUTHENTICATION_SECRETS = ( <secret_name_1> [, <secret_name_2>, ... ] ) ]
      ENABLED = { TRUE | FALSE }
      [ COMMENT = '<string_literal>' ]
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
        allowed_network_rules: list[NetworkRule] = [],
        allowed_api_authentication_integrations: list[str] = None,
        allowed_authentication_secrets: list[Secret] = None,
        enabled: bool = True,
        comment: str = None,
        owner: str = "ACCOUNTADMIN",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._data: _ExternalAccessIntegration = _ExternalAccessIntegration(
            name=name,
            allowed_network_rules=allowed_network_rules,
            allowed_api_authentication_integrations=allowed_api_authentication_integrations,
            allowed_authentication_secrets=allowed_authentication_secrets,
            enabled=enabled,
            comment=comment,
            owner=owner,
        )
        self.requires(self._data.allowed_network_rules)
        self.requires(self._data.allowed_authentication_secrets)
