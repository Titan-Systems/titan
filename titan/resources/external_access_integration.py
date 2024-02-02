from dataclasses import dataclass

from .resource import Resource, ResourceSpec
from .network_rule import NetworkRule
from .secret import Secret
from ..enums import ResourceType
from ..scope import AccountScope

from ..props import BoolProp, Props, StringProp, IdentifierListProp


@dataclass
class _ExternalAccessIntegration(ResourceSpec):
    name: str
    allowed_network_rules: list[str]  # NetworkRule
    allowed_api_authentication_integrations: list[str] = None
    allowed_authentication_secrets: list[str] = None  # Secret
    enabled: bool = True
    comment: str = None
    owner: str = "ACCOUNTADMIN"


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
