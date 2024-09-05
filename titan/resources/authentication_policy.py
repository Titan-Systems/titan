from dataclasses import dataclass

from ..enums import ParseableEnum, ResourceType
from ..props import EnumProp, Props, StringListProp, StringProp
from ..resource_name import ResourceName
from ..role_ref import RoleRef
from ..scope import SchemaScope
from .resource import NamedResource, Resource, ResourceSpec


class AuthenticationMethods(ParseableEnum):
    ALL = "ALL"
    PASSWORD = "PASSWORD"
    SAML = "SAML"
    OAUTH = "OAUTH"
    KEYPAIR = "KEYPAIR"


class MFAEnrollment(ParseableEnum):
    REQUIRED = "REQUIRED"
    OPTIONAL = "OPTIONAL"


class ClientTypes(ParseableEnum):
    ALL = "ALL"
    SNOWFLAKE_UI = "SNOWFLAKE_UI"
    DRIVERS = "DRIVERS"
    SNOWSQL = "SNOWSQL"


@dataclass(unsafe_hash=True)
class _AuthenticationPolicy(ResourceSpec):
    name: ResourceName
    authentication_methods: list[AuthenticationMethods] = None
    mfa_authentication_methods: list[AuthenticationMethods] = None
    mfa_enrollment: MFAEnrollment = "OPTIONAL"
    client_types: list[ClientTypes] = None
    security_integrations: list[str] = None
    comment: str = None
    owner: RoleRef = "SECURITYADMIN"

    def __post_init__(self):
        super().__post_init__()
        if self.authentication_methods is None:
            self.authentication_methods = [AuthenticationMethods.ALL]

        if self.mfa_authentication_methods is None:
            self.mfa_authentication_methods = [AuthenticationMethods.PASSWORD, AuthenticationMethods.SAML]
        else:
            for method in self.mfa_authentication_methods:
                if method not in (
                    AuthenticationMethods.ALL,
                    AuthenticationMethods.SAML,
                    AuthenticationMethods.PASSWORD,
                ):
                    raise ValueError("MFA authentication methods must be either 'ALL', 'SAML', or 'PASSWORD'")
            if (
                len(self.mfa_authentication_methods) == 1
                and self.mfa_authentication_methods[0] == AuthenticationMethods.ALL
            ):
                self.mfa_authentication_methods = [AuthenticationMethods.PASSWORD, AuthenticationMethods.SAML]

        if self.client_types is None:
            self.client_types = [ClientTypes.ALL]

        if self.security_integrations is None:
            self.security_integrations = ["ALL"]


class AuthenticationPolicy(NamedResource, Resource):
    """
    Description:
        Defines the rules and constraints for authentication within the system, ensuring they meet specific security standards.

    Snowflake Docs:
        https://docs.snowflake.com/en/sql-reference/sql/create-authentication-policy

    Fields:
        name (string, required): The name of the authentication policy.
        authentication_methods (list): A list of allowed authentication methods.
        mfa_authentication_methods (list): A list of authentication methods that enforce multi-factor authentication (MFA).
        mfa_enrollment (string): Determines whether a user must enroll in multi-factor authentication. Defaults to OPTIONAL.
        client_types (list): A list of clients that can authenticate with Snowflake.
        security_integrations (list): A list of security integrations the authentication policy is associated with.
        comment (string): A comment or description for the authentication policy.
        owner (string or Role): The owner role of the authentication policy. Defaults to SECURITYADMIN.

    Python:

        ```python
        authentication_policy = AuthenticationPolicy(
            name="some_authentication_policy",
            authentication_methods=["PASSWORD", "SAML"],
            mfa_authentication_methods=["PASSWORD"],
            mfa_enrollment="REQUIRED",
            client_types=["SNOWFLAKE_UI"],
            security_integrations=["ALL"],
            comment="Policy for secure authentication."
        )
        ```

    Yaml:

        ```yaml
        authentication_policies:
          - name: some_authentication_policy
            authentication_methods:
              - PASSWORD
              - SAML
            mfa_authentication_methods:
              - PASSWORD
            mfa_enrollment: REQUIRED
            client_types:
              - SNOWFLAKE_UI
            security_integrations:
              - ALL
            comment: Policy for secure authentication.
        ```
    """

    resource_type = ResourceType.AUTHENTICATION_POLICY
    props = Props(
        authentication_methods=StringListProp("authentication_methods", parens=True),
        mfa_authentication_methods=StringListProp("mfa_authentication_methods", parens=True),
        mfa_enrollment=EnumProp("mfa_enrollment", MFAEnrollment),
        client_types=StringListProp("client_types", parens=True),
        security_integrations=StringListProp("security_integrations", parens=True),
        comment=StringProp("comment"),
    )
    scope = SchemaScope()
    spec = _AuthenticationPolicy

    def __init__(
        self,
        name: str,
        authentication_methods: list[str] = None,
        mfa_authentication_methods: list[str] = None,
        mfa_enrollment: str = "OPTIONAL",
        client_types: list[str] = None,
        security_integrations: list[str] = None,
        comment: str = None,
        owner: str = "SECURITYADMIN",
        **kwargs,
    ):
        super().__init__(name, **kwargs)
        self._data: _AuthenticationPolicy = _AuthenticationPolicy(
            name=self._name,
            authentication_methods=authentication_methods,
            mfa_authentication_methods=mfa_authentication_methods,
            mfa_enrollment=mfa_enrollment,
            client_types=client_types,
            security_integrations=security_integrations,
            comment=comment,
            owner=owner,
        )
