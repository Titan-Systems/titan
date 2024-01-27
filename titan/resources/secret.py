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


class SecretType(ParseableEnum):
    OAUTH2 = "OAUTH2"
    PASSWORD = "PASSWORD"
    GENERIC_STRING = "GENERIC_STRING"


@dataclass
class _Secret(ResourceSpec):
    name: str
    type: SecretType
    api_authentication: str
    oauth_scopes: list[str] = None
    oauth_refresh_token: str = None
    oauth_refresh_token_expiry_time: str = None
    username: str = None
    password: str = None
    secret_string: str = None
    comment: str = None
    owner: str = "SYSADMIN"

    def __post_init__(self):
        if self.type == SecretType.OAUTH2 and not self.api_authentication:
            raise ValueError("api_authentication must be set when type is OAUTH2")
        if self.type != SecretType.OAUTH2 and self.api_authentication:
            raise ValueError("api_authentication must not be set when type is not OAUTH2")


class Secret(Resource):
    """
    A Secret defines a set of sensitive data that can be used for authentication or other purposes.

    CREATE [ OR REPLACE ] SECRET <name>
       TYPE = { OAUTH2 | PASSWORD | GENERIC_STRING }
       API_AUTHENTICATION = '<security_integration_name>'
       [ OAUTH_SCOPES = ( '<scope_1>' [, '<scope_2>', ... ] ) ]
       [ OAUTH_REFRESH_TOKEN = '<string_literal>' ]
       [ OAUTH_REFRESH_TOKEN_EXPIRY_TIME = '<string_literal>' ]
       [ USERNAME = '<username>' ]
       [ PASSWORD = '<password>' ]
       [ SECRET_STRING = '<string_literal>' ]
       [ COMMENT = '<string_literal>' ]
    """

    resource_type = ResourceType.SECRET
    props = Props(
        type=EnumProp("type", SecretType),
        api_authentication=StringProp("api_authentication"),
        oauth_scopes=StringListProp("oauth_scopes", parens=True),
        oauth_refresh_token=StringProp("oauth_refresh_token"),
        oauth_refresh_token_expiry_time=StringProp("oauth_refresh_token_expiry_time"),
        username=StringProp("username"),
        password=StringProp("password"),
        secret_string=StringProp("secret_string"),
        comment=StringProp("comment"),
    )
    scope = SchemaScope()
    spec = _Secret

    def __init__(
        self,
        name: str,
        type: SecretType,
        api_authentication: str = None,
        oauth_scopes: list[str] = None,
        oauth_refresh_token: str = None,
        oauth_refresh_token_expiry_time: str = None,
        username: str = None,
        password: str = None,
        secret_string: str = None,
        comment: str = None,
        owner: str = "SYSADMIN",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._data: _Secret = _Secret(
            name=name,
            type=type,
            api_authentication=api_authentication,
            oauth_scopes=oauth_scopes,
            oauth_refresh_token=oauth_refresh_token,
            oauth_refresh_token_expiry_time=oauth_refresh_token_expiry_time,
            username=username,
            password=password,
            secret_string=secret_string,
            comment=comment,
            owner=owner,
        )
