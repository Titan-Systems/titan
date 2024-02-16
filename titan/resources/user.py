from dataclasses import dataclass

from .resource import Resource, ResourceSpec
from ..enums import ResourceType
from ..props import Props, BoolProp, IntProp, StringProp, StringListProp, TagsProp
from ..scope import AccountScope


@dataclass(unsafe_hash=True)
class _User(ResourceSpec):
    name: str
    owner: str = "USERADMIN"
    password: str = None
    login_name: str = None
    display_name: str = None
    first_name: str = None
    middle_name: str = None
    last_name: str = None
    email: str = None
    must_change_password: bool = False
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
    tags: dict[str, str] = None

    def __post_init__(self):
        super().__post_init__()
        if not self.login_name:
            self.login_name = self.name
        if not self.display_name:
            self.display_name = self.name


class User(Resource):
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
        must_change_password: bool = False,
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
        tags: dict[str, str] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        name = name.upper()
        self._data: _User = _User(
            name=name,
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
            tags=tags,
        )

    @property
    def name(self):
        return self._data.name
