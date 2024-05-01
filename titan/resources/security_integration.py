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


class OAuthClient(ParseableEnum):
    CUSTOM = "CUSTOM"
    LOOKER = "LOOKER"
    SNOWSERVICES_INGRESS = "SNOWSERVICES_INGRESS"
    TABLEAU_DESKTOP = "TABLEAU_DESKTOP"
    TABLEAU_SERVER = "TABLEAU_SERVER"


@dataclass(unsafe_hash=True)
class _SecurityIntegration(ResourceSpec):
    name: ResourceName
    integration_type: str = "OAUTH"
    enabled: bool = True
    oauth_client: OAuthClient = None
    oauth_client_secret: str = None
    oauth_redirect_uri: str = None
    oauth_issue_refresh_tokens: bool = True
    oauth_refresh_token_validity: int = None
    comment: str = None


class SecurityIntegration(Resource):
    """A security integration in Snowflake to manage external authentication mechanisms."""

    resource_type = ResourceType.SECURITY_INTEGRATION
    props = Props(
        integration_type=StringProp("integration_type"),
        enabled=BoolProp("enabled"),
        oauth_client=EnumProp("oauth_client", OAuthClient),
        oauth_client_secret=StringProp("oauth_client_secret"),
        oauth_redirect_uri=StringProp("oauth_redirect_uri"),
        oauth_issue_refresh_tokens=BoolProp("oauth_issue_refresh_tokens"),
        oauth_refresh_token_validity=IntProp("oauth_refresh_token_validity"),
        comment=StringProp("comment"),
    )
    scope = AccountScope()
    spec = _SecurityIntegration

    def __init__(
        self,
        name: str,
        integration_type: str = "OAUTH",
        enabled: bool = True,
        oauth_client: OAuthClient = None,
        oauth_client_secret: str = None,
        oauth_redirect_uri: str = None,
        oauth_issue_refresh_tokens: bool = True,
        oauth_refresh_token_validity: int = None,
        comment: str = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._data = _SecurityIntegration(
            name=name,
            integration_type=integration_type,
            enabled=enabled,
            oauth_client=oauth_client,
            oauth_client_secret=oauth_client_secret,
            oauth_redirect_uri=oauth_redirect_uri,
            oauth_issue_refresh_tokens=oauth_issue_refresh_tokens,
            oauth_refresh_token_validity=oauth_refresh_token_validity,
            comment=comment,
        )
