from dataclasses import dataclass, field

from .resource import Resource, ResourceSpec
from .role import Role
from ..enums import ParseableEnum, ResourceType
from ..resource_name import ResourceName
from ..scope import AccountScope

from ..props import (
    BoolProp,
    StringProp,
    Props,
    IntProp,
    EnumProp,
)


class SecurityIntegrationType(ParseableEnum):
    API_AUTHENTICATION = "API_AUTHENTICATION"
    EXTERNAL_OAUTH = "EXTERNAL_OAUTH"
    OAUTH = "OAUTH"
    SAML2 = "SAML"
    SCIM = "SCIM"


class OAuthClient(ParseableEnum):
    CUSTOM = "CUSTOM"
    LOOKER = "LOOKER"
    SNOWSERVICES_INGRESS = "SNOWSERVICES_INGRESS"
    TABLEAU_DESKTOP = "TABLEAU_DESKTOP"
    TABLEAU_SERVER = "TABLEAU_SERVER"


@dataclass(unsafe_hash=True)
class _SnowflakePartnerOAuthSecurityIntegration(ResourceSpec):
    name: ResourceName
    type: SecurityIntegrationType = SecurityIntegrationType.OAUTH
    enabled: bool = True
    oauth_client: OAuthClient = None
    oauth_client_secret: str = field(default_factory=None, metadata={"fetchable": False})
    oauth_redirect_uri: str = field(default_factory=None, metadata={"fetchable": False})
    oauth_issue_refresh_tokens: bool = True
    oauth_refresh_token_validity: int = 7776000
    comment: str = None

    def __post_init__(self):
        super().__post_init__()
        if self.oauth_client not in [OAuthClient.LOOKER, OAuthClient.TABLEAU_DESKTOP, OAuthClient.TABLEAU_SERVER]:
            raise ValueError(f"Invalid OAuth client: {self.oauth_client}")


class SnowflakePartnerOAuthSecurityIntegration(Resource):
    """
    Description:
        A security integration in Snowflake designed to manage external OAuth clients for authentication purposes.
        This integration supports specific OAuth clients such as Looker, Tableau Desktop, and Tableau Server.

    Snowflake Docs:
        https://docs.snowflake.com/en/sql-reference/sql/create-security-integration.html

    Fields:
        name (string, required): The name of the security integration.
        enabled (bool): Specifies if the security integration is enabled. Defaults to True.
        oauth_client (string or OAuthClient): The OAuth client used for authentication. Supported clients are 'LOOKER', 'TABLEAU_DESKTOP', and 'TABLEAU_SERVER'.
        oauth_client_secret (string): The secret associated with the OAuth client.
        oauth_redirect_uri (string): The redirect URI configured for the OAuth client.
        oauth_issue_refresh_tokens (bool): Indicates if refresh tokens should be issued. Defaults to True.
        oauth_refresh_token_validity (int): The validity period of the refresh token in seconds.
        comment (string): A comment about the security integration.

    Python:

        ```python
        snowflake_partner_oauth_security_integration = SnowflakePartnerOAuthSecurityIntegration(
            name="some_security_integration",
            enabled=True,
            oauth_client="LOOKER",
            oauth_client_secret="secret123",
            oauth_redirect_uri="https://example.com/oauth/callback",
            oauth_issue_refresh_tokens=True,
            oauth_refresh_token_validity=7776000,
            comment="Integration for Looker OAuth"
        )
        ```

    Yaml:

        ```yaml
        security_integrations:
          - name: some_security_integration
            enabled: true
            oauth_client: LOOKER
            oauth_client_secret: secret123
            oauth_redirect_uri: https://example.com/oauth/callback
            oauth_issue_refresh_tokens: true
            oauth_refresh_token_validity: 7776000
            comment: Integration for Looker OAuth
        ```
    """

    resource_type = ResourceType.SECURITY_INTEGRATION
    props = Props(
        type=EnumProp("type", [SecurityIntegrationType.OAUTH]),
        enabled=BoolProp("enabled"),
        oauth_client=EnumProp(
            "oauth_client", [OAuthClient.LOOKER, OAuthClient.TABLEAU_DESKTOP, OAuthClient.TABLEAU_SERVER]
        ),
        oauth_client_secret=StringProp("oauth_client_secret"),
        oauth_redirect_uri=StringProp("oauth_redirect_uri"),
        oauth_issue_refresh_tokens=BoolProp("oauth_issue_refresh_tokens"),
        oauth_refresh_token_validity=IntProp("oauth_refresh_token_validity"),
        comment=StringProp("comment"),
    )
    scope = AccountScope()
    spec = _SnowflakePartnerOAuthSecurityIntegration

    def __init__(
        self,
        name: str,
        enabled: bool = True,
        oauth_client: OAuthClient = None,
        oauth_client_secret: str = None,
        oauth_redirect_uri: str = None,
        oauth_issue_refresh_tokens: bool = True,
        oauth_refresh_token_validity: int = None,
        comment: str = None,
        **kwargs,
    ):
        kwargs.pop("type", None)
        super().__init__(**kwargs)
        self._data = _SnowflakePartnerOAuthSecurityIntegration(
            name=name,
            enabled=enabled,
            oauth_client=oauth_client,
            oauth_client_secret=oauth_client_secret,
            oauth_redirect_uri=oauth_redirect_uri,
            oauth_issue_refresh_tokens=oauth_issue_refresh_tokens,
            oauth_refresh_token_validity=oauth_refresh_token_validity,
            comment=comment,
        )


@dataclass(unsafe_hash=True)
class _SnowservicesOAuthSecurityIntegration(ResourceSpec):
    name: ResourceName
    type: SecurityIntegrationType = SecurityIntegrationType.OAUTH
    oauth_client: OAuthClient = OAuthClient.SNOWSERVICES_INGRESS
    enabled: bool = True
    comment: str = None


class SnowservicesOAuthSecurityIntegration(Resource):
    """
    Description:
        Manages OAuth security integrations for Snowservices in Snowflake, allowing external authentication mechanisms.

    Snowflake Docs:
        https://docs.snowflake.com/en/sql-reference/sql/create-security-integration.html

    Fields:
        name (string, required): The name of the security integration.
        enabled (bool): Specifies if the security integration is enabled. Defaults to True.
        comment (string): A comment about the security integration.

    Python:

        ```python
        snowservices_oauth = SnowservicesOAuthSecurityIntegration(
            name="some_security_integration",
            enabled=True,
            comment="Integration for external OAuth services."
        )
        ```

    Yaml:

        ```yaml
        snowservices_oauth:
          - name: some_security_integration
            enabled: true
            comment: Integration for external OAuth services.
        ```
    """

    resource_type = ResourceType.SECURITY_INTEGRATION
    props = Props(
        type=EnumProp("type", [SecurityIntegrationType.OAUTH]),
        oauth_client=EnumProp("oauth_client", [OAuthClient.SNOWSERVICES_INGRESS]),
        enabled=BoolProp("enabled"),
        comment=StringProp("comment"),
    )
    scope = AccountScope()
    spec = _SnowservicesOAuthSecurityIntegration

    def __init__(
        self,
        name: str,
        enabled: bool = True,
        comment: str = None,
        **kwargs,
    ):
        kwargs.pop("type", None)
        kwargs.pop("oauth_client", None)
        super().__init__(**kwargs)
        self._data = _SnowservicesOAuthSecurityIntegration(
            name=name,
            enabled=enabled,
            comment=comment,
        )


def _resolver(data: dict):
    security_integration_type = SecurityIntegrationType(data["type"])
    if security_integration_type == SecurityIntegrationType.OAUTH:
        oauth_client = OAuthClient(data["oauth_client"])
        if oauth_client in [
            OAuthClient.LOOKER,
            OAuthClient.TABLEAU_DESKTOP,
            OAuthClient.TABLEAU_SERVER,
        ]:
            return SnowflakePartnerOAuthSecurityIntegration
        elif oauth_client == OAuthClient.CUSTOM:
            # return SnowflakeCustomOAuthSecurityIntegration
            return None
        elif oauth_client == OAuthClient.SNOWSERVICES_INGRESS:
            return SnowservicesOAuthSecurityIntegration
    return None


Resource.__resolvers__[ResourceType.SECURITY_INTEGRATION] = _resolver
