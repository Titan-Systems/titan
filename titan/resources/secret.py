from dataclasses import dataclass, field

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


class SecretType(ParseableEnum):
    OAUTH2 = "OAUTH2"
    PASSWORD = "PASSWORD"
    GENERIC_STRING = "GENERIC_STRING"


@dataclass(unsafe_hash=True)
class _PasswordSecret(ResourceSpec):
    name: ResourceName
    secret_type: SecretType = SecretType.PASSWORD
    username: str = None
    password: str = field(default=None, metadata={"fetchable": False})
    comment: str = None
    owner: RoleRef = "SYSADMIN"


class PasswordSecret(NamedResource, Resource):
    """
    Description:
        A Secret defines a set of sensitive data that can be used for authentication or other purposes.
        This class defines a password secret.

    Snowflake Docs:
        https://docs.snowflake.com/en/sql-reference/sql/create-secret

    Fields:
        name (string, required): The name of the secret.
        username (string): The username for the secret.
        password (string): The password for the secret.
        comment (string): A comment for the secret.
        owner (string or Role): The owner of the secret. Defaults to SYSADMIN.

    Python:

        ```python
        secret = PasswordSecret(
            name="some_secret",
            username="some_username",
            password="some_password",
            comment="some_comment",
            owner="SYSADMIN",
        )
        ```

    Yaml:

        ```yaml
        secrets:
          - name: some_secret
            secret_type: PASSWORD
            username: some_username
            password: some_password
            comment: some_comment
            owner: SYSADMIN
        ```
    """

    resource_type = ResourceType.SECRET
    props = Props(
        secret_type=EnumProp("type", SecretType),
        username=StringProp("username"),
        password=StringProp("password"),
        comment=StringProp("comment"),
    )
    scope = SchemaScope()
    spec = _PasswordSecret

    def __init__(
        self,
        name: str,
        username: str,
        password: str,
        comment: str = None,
        owner: str = "SYSADMIN",
        **kwargs,
    ):
        kwargs.pop("secret_type", None)
        super().__init__(name, **kwargs)
        self._data: _PasswordSecret = _PasswordSecret(
            name=self._name,
            username=username,
            password=password,
            comment=comment,
            owner=owner,
        )


@dataclass(unsafe_hash=True)
class _GenericSecret(ResourceSpec):
    name: ResourceName
    secret_type: SecretType = SecretType.GENERIC_STRING
    secret_string: str = field(default=None, metadata={"fetchable": False})
    comment: str = None
    owner: RoleRef = "SYSADMIN"


class GenericSecret(NamedResource, Resource):
    """
    Description:
        A Secret defines a set of sensitive data that can be used for authentication or other purposes.
        This class defines a generic secret.

    Snowflake Docs:
        https://docs.snowflake.com/en/sql-reference/sql/create-secret

    Fields:
        name (string, required): The name of the secret.
        secret_string (string): The secret string.
        comment (string): A comment for the secret.
        owner (string or Role): The owner of the secret. Defaults to SYSADMIN.

    Python:

        ```python
        secret = GenericSecret(
            name="some_secret",
            secret_string="some_secret_string",
            comment="some_comment",
            owner="SYSADMIN",
        )
        ```

    Yaml:

        ```yaml
        secrets:
          - name: some_secret
            secret_type: GENERIC_STRING
            secret_string: some_secret_string
            comment: some_comment
            owner: SYSADMIN
        ```
    """

    resource_type = ResourceType.SECRET
    props = Props(
        secret_type=EnumProp("type", SecretType),
        secret_string=StringProp("secret_string"),
        comment=StringProp("comment"),
    )
    scope = SchemaScope()
    spec = _GenericSecret

    def __init__(
        self,
        name: str,
        secret_string: str,
        comment: str = None,
        owner: str = "SYSADMIN",
        **kwargs,
    ):
        kwargs.pop("secret_type", None)
        super().__init__(name, **kwargs)
        self._data: _GenericSecret = _GenericSecret(
            name=self._name,
            secret_string=secret_string,
            comment=comment,
            owner=owner,
        )


@dataclass(unsafe_hash=True)
class _OAuthSecret(ResourceSpec):
    name: ResourceName
    api_authentication: str
    secret_type: SecretType = SecretType.OAUTH2
    oauth_scopes: list[str] = None
    oauth_refresh_token: str = field(default=None, metadata={"fetchable": False})
    oauth_refresh_token_expiry_time: str = None
    comment: str = None
    owner: RoleRef = "SYSADMIN"

    def __post_init__(self):
        super().__post_init__()
        if self.oauth_scopes and any((self.oauth_refresh_token, self.oauth_refresh_token_expiry_time)):
            raise ValueError("Cannot specify both oauth_scopes and oauth_refresh_token.")
        if self.oauth_refresh_token and not self.oauth_refresh_token_expiry_time:
            raise ValueError("Expiry time must be provided if refresh token is specified.")


class OAuthSecret(NamedResource, Resource):
    """
    Description:
        A Secret defines a set of sensitive data that can be used for authentication or other purposes.
        This class defines an OAuth secret.

    Snowflake Docs:
        https://docs.snowflake.com/en/sql-reference/sql/create-secret

    Fields:
        name (string, required): The name of the secret.
        api_authentication (string): The security integration name for API authentication.
        oauth_scopes (list): The OAuth scopes for the secret.
        oauth_refresh_token (string): The OAuth refresh token.
        oauth_refresh_token_expiry_time (string): The expiry time of the OAuth refresh token.
        comment (string): A comment for the secret.
        owner (string or Role): The owner of the secret. Defaults to SYSADMIN.

    Python:

        ```python
        # OAuth with client credentials flow:
        secret = OAuthSecret(
            name="some_secret",
            api_authentication="some_security_integration",
            oauth_scopes=["scope1", "scope2"],
            comment="some_comment",
            owner="SYSADMIN",
        )

        # OAuth with authorization code grant flow:
        secret = OAuthSecret(
            name="another_secret",
            api_authentication="some_security_integration",
            oauth_refresh_token="34n;vods4nQsdg09wee4qnfvadH",
            oauth_refresh_token_expiry_time="2049-01-06 20:00:00",
            comment="some_comment",
            owner="SYSADMIN",
        )
        ```

    Yaml:

        ```yaml
        secrets:
          - name: some_secret
            secret_type: OAUTH2
            api_authentication: some_security_integration
            oauth_scopes:
              - scope1
              - scope2
            comment: some_comment
            owner: SYSADMIN
          - name: another_secret
            secret_type: OAUTH2
            api_authentication: some_security_integration
            oauth_refresh_token: 34n;vods4nQsdg09wee4qnfvadH
            oauth_refresh_token_expiry_time: 2049-01-06 20:00:00
            comment: some_comment
            owner: SYSADMIN
        ```
    """

    resource_type = ResourceType.SECRET
    props = Props(
        secret_type=EnumProp("type", SecretType),
        api_authentication=StringProp("api_authentication"),
        oauth_scopes=StringListProp("oauth_scopes", parens=True),
        oauth_refresh_token=StringProp("oauth_refresh_token"),
        oauth_refresh_token_expiry_time=StringProp("oauth_refresh_token_expiry_time"),
        comment=StringProp("comment"),
    )
    scope = SchemaScope()
    spec = _OAuthSecret

    def __init__(
        self,
        name: str,
        api_authentication: str,
        oauth_scopes: list[str] = None,
        oauth_refresh_token: str = None,
        oauth_refresh_token_expiry_time: str = None,
        comment: str = None,
        owner: str = "SYSADMIN",
        **kwargs,
    ):
        kwargs.pop("secret_type", None)
        super().__init__(name, **kwargs)
        self._data: _OAuthSecret = _OAuthSecret(
            name=self._name,
            api_authentication=api_authentication,
            oauth_scopes=oauth_scopes,
            oauth_refresh_token=oauth_refresh_token,
            oauth_refresh_token_expiry_time=oauth_refresh_token_expiry_time,
            comment=comment,
            owner=owner,
        )

    def to_dict(self):
        data = super().to_dict()
        if data["oauth_scopes"]:
            data.pop("oauth_refresh_token")
            data.pop("oauth_refresh_token_expiry_time")
        else:
            data.pop("oauth_scopes")
        return data


SecretMap = {
    SecretType.PASSWORD: PasswordSecret,
    SecretType.OAUTH2: OAuthSecret,
    SecretType.GENERIC_STRING: GenericSecret,
}


def _resolver(data: dict):
    return SecretMap[SecretType(data["secret_type"])]


Resource.__resolvers__[ResourceType.SECRET] = _resolver
