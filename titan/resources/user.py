import logging
from dataclasses import dataclass

from ..enums import ParseableEnum, ResourceType
from ..props import BoolProp, EnumProp, IntProp, Props, StringListProp, StringProp, TagsProp
from ..resource_name import ResourceName
from ..scope import AccountScope
from .resource import NamedResource, Resource, ResourceSpec
from .role import Role
from .tag import TaggableResource

logger = logging.getLogger("titan")


class UserType(ParseableEnum):
    PERSON = "PERSON"
    SERVICE = "SERVICE"
    LEGACY_SERVICE = "LEGACY_SERVICE"
    NULL = "NULL"


@dataclass(unsafe_hash=True)
class _User(ResourceSpec):
    name: ResourceName
    owner: Role = "USERADMIN"
    password: str = None
    login_name: str = None
    display_name: str = None
    first_name: str = None
    middle_name: str = None
    last_name: str = None
    email: str = None
    must_change_password: bool = None
    disabled: bool = False
    days_to_expiry: int = None
    mins_to_unlock: int = None
    default_warehouse: str = None
    default_namespace: str = None
    default_role: str = None
    default_secondary_roles: list[str] = None
    mins_to_bypass_mfa: int = None
    rsa_public_key: str = None
    rsa_public_key_2: str = None
    comment: str = None
    network_policy: str = None
    type: UserType = "NULL"

    def __post_init__(self):
        super().__post_init__()

        if self.type is None:
            self.type = UserType.NULL

        if self.type == UserType.SERVICE:
            if self.first_name is not None:
                raise ValueError("First name is not supported for service users")
            if self.middle_name is not None:
                raise ValueError("Middle name is not supported for service users")
            if self.last_name is not None:
                raise ValueError("Last name is not supported for service users")
            if self.password is not None:
                raise ValueError("Password is not supported for service users")
            if self.must_change_password is not None:
                raise ValueError("Must change password is not supported for service users")
            if self.mins_to_bypass_mfa is not None:
                raise ValueError("Mins to bypass MFA is not supported for service users")

        else:
            if self.login_name is None or self.login_name == "":
                self.login_name = self.name._name.upper()
            if self.display_name is None:
                self.display_name = self.name._name
            if self.must_change_password is None:
                self.must_change_password = False


class User(NamedResource, TaggableResource, Resource):
    """
    Description:
        A user in Snowflake.

    Snowflake Docs:
        https://docs.snowflake.com/en/sql-reference/sql/create-user

    Fields:
        name (string, required): The name of the user.
        owner (string or Role): The owner of the user. Defaults to "USERADMIN".
        password (string): The password of the user.
        login_name (string): The login name of the user. Defaults to the name in uppercase.
        display_name (string): The display name of the user. Defaults to the name in lowercase.
        first_name (string): The first name of the user.
        middle_name (string): The middle name of the user.
        last_name (string): The last name of the user.
        email (string): The email of the user.
        must_change_password (bool): Whether the user must change their password. Defaults to False.
        disabled (bool): Whether the user is disabled. Defaults to False.
        days_to_expiry (int): The number of days until the user's password expires.
        mins_to_unlock (int): The number of minutes until the user's account is unlocked.
        default_warehouse (string): The default warehouse for the user.
        default_namespace (string): The default namespace for the user.
        default_role (string): The default role for the user.
        default_secondary_roles (list): The default secondary roles for the user.
        mins_to_bypass_mfa (int): The number of minutes until the user can bypass Multi-Factor Authentication.
        rsa_public_key (string): The RSA public key for the user.
        rsa_public_key_2 (string): The RSA public key for the user.
        comment (string): A comment for the user.
        network_policy (string): The network policy for the user.
        type (string or UserType): The type of the user. Defaults to "NULL".
        tags (dict): Tags for the user.

    Python:

        ```python
        user = User(
            name="some_user",
            owner="USERADMIN",
            email="some.user@example.com",
            type="PERSON",
        )
        ```

    Yaml:

        ```yaml
        users:
          - name: some_user
            owner: USERADMIN
            email: some.user@example.com
            type: PERSON
        ```

    """

    resource_type = ResourceType.USER
    props = Props(
        password=StringProp("password"),
        login_name=StringProp("login_name"),
        display_name=StringProp("display_name"),
        first_name=StringProp("first_name"),
        middle_name=StringProp("middle_name"),
        last_name=StringProp("last_name"),
        email=StringProp("email"),
        must_change_password=BoolProp("must_change_password"),
        disabled=BoolProp("disabled"),
        days_to_expiry=IntProp("days_to_expiry"),
        mins_to_unlock=IntProp("mins_to_unlock"),
        default_warehouse=StringProp("default_warehouse"),
        default_namespace=StringProp("default_namespace"),
        default_role=StringProp("default_role"),
        default_secondary_roles=StringListProp("default_secondary_roles", parens=True),
        mins_to_bypass_mfa=IntProp("mins_to_bypass_mfa"),
        rsa_public_key=StringProp("rsa_public_key"),
        rsa_public_key_2=StringProp("rsa_public_key_2"),
        comment=StringProp("comment"),
        network_policy=StringProp("network_policy"),
        type=EnumProp("type", UserType),
        tags=TagsProp(),
    )
    scope = AccountScope()
    spec = _User

    def __init__(
        self,
        name: str,
        owner: str = "USERADMIN",
        password: str = None,
        login_name: str = None,
        display_name: str = None,
        first_name: str = None,
        middle_name: str = None,
        last_name: str = None,
        email: str = None,
        must_change_password: bool = None,
        disabled: bool = False,
        days_to_expiry: int = None,
        mins_to_unlock: int = None,
        default_warehouse: str = None,
        default_namespace: str = None,
        default_role: str = None,
        default_secondary_roles: list[str] = None,
        mins_to_bypass_mfa: int = None,
        rsa_public_key: str = None,
        rsa_public_key_2: str = None,
        comment: str = None,
        network_policy: str = None,
        type: str = None,
        tags: dict[str, str] = None,
        **kwargs,
    ):

        user_type = kwargs.pop("user_type", None)
        if user_type:
            logging.warning("The 'user_type' parameter is deprecated. Use 'type' instead.")
            if type is None:
                type = user_type
            else:
                raise ValueError("Both 'type' and 'user_type' parameters are set. Use only 'type'.")

        super().__init__(name, **kwargs)
        self._data: _User = _User(
            name=self._name,
            owner=owner,
            password=password,
            login_name=login_name,
            display_name=display_name,
            first_name=first_name,
            middle_name=middle_name,
            last_name=last_name,
            email=email,
            must_change_password=must_change_password,
            disabled=disabled,
            days_to_expiry=days_to_expiry,
            mins_to_unlock=mins_to_unlock,
            default_warehouse=default_warehouse,
            default_namespace=default_namespace,
            default_role=default_role,
            default_secondary_roles=default_secondary_roles,
            mins_to_bypass_mfa=mins_to_bypass_mfa,
            rsa_public_key=rsa_public_key,
            rsa_public_key_2=rsa_public_key_2,
            comment=comment,
            network_policy=network_policy,
            type=type,
        )
        self.set_tags(tags)

    @property
    def owner(self):
        return self._data.owner
