from dataclasses import dataclass

from .resource import Resource, ResourceSpec
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
class _SnowflakeOAuthSecurityIntegration(ResourceSpec):
    name: ResourceName
    type: SecurityIntegrationType = SecurityIntegrationType.OAUTH
    enabled: bool = True
    oauth_client: OAuthClient = None
    oauth_client_secret: str = None
    oauth_redirect_uri: str = None
    oauth_issue_refresh_tokens: bool = True
    oauth_refresh_token_validity: int = None
    comment: str = None


class SnowflakeOAuthSecurityIntegration(Resource):
    """A security integration in Snowflake to manage external authentication mechanisms."""

    resource_type = ResourceType.SECURITY_INTEGRATION
    props = Props(
        type=EnumProp("type", [SecurityIntegrationType.OAUTH]),
        enabled=BoolProp("enabled"),
        oauth_client=EnumProp("oauth_client", OAuthClient),
        oauth_client_secret=StringProp("oauth_client_secret"),
        oauth_redirect_uri=StringProp("oauth_redirect_uri"),
        oauth_issue_refresh_tokens=BoolProp("oauth_issue_refresh_tokens"),
        oauth_refresh_token_validity=IntProp("oauth_refresh_token_validity"),
        comment=StringProp("comment"),
    )
    scope = AccountScope()
    spec = _SnowflakeOAuthSecurityIntegration

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
        self._data = _SnowflakeOAuthSecurityIntegration(
            name=name,
            enabled=enabled,
            oauth_client=oauth_client,
            oauth_client_secret=oauth_client_secret,
            oauth_redirect_uri=oauth_redirect_uri,
            oauth_issue_refresh_tokens=oauth_issue_refresh_tokens,
            oauth_refresh_token_validity=oauth_refresh_token_validity,
            comment=comment,
        )


SecurityIntegrationMap = {
    # SecurityIntegrationType.API_AUTHENTICATION: SecurityIntegration,
    # SecurityIntegrationType.EXTERNAL_OAUTH: SecurityIntegration,
    SecurityIntegrationType.OAUTH: SnowflakeOAuthSecurityIntegration,
    # SecurityIntegrationType.SAML2: SecurityIntegration,
    # SecurityIntegrationType.SCIM: SecurityIntegration,
}


def _resolver(data: dict):
    return SecurityIntegrationMap[SecurityIntegrationType(data["integration_type"])]


Resource.__resolvers__[ResourceType.STORAGE_INTEGRATION] = _resolver
