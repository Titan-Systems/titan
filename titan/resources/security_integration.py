from dataclasses import dataclass, field

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
    """A security integration in Snowflake to manage external authentication mechanisms."""

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
    """A security integration in Snowflake to manage external authentication mechanisms."""

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
