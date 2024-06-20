from dataclasses import dataclass, field
from typing import Any, Union

from inflection import singularize

from .resource import Resource, ResourcePointer, ResourceSpec
from .role import Role
from .user import User
from ..enums import ParseableEnum, ResourceType
from ..identifiers import FQN, resource_label_for_type, resource_type_for_label
from ..parse import _parse_grant
from ..privs import GlobalPriv, GLOBAL_PRIV_DEFAULT_OWNERS, _all_privs_for_resource_type
from ..props import Props, FlagProp, IdentifierProp
from ..resource_name import ResourceName
from ..scope import AccountScope

# TODO: Should Grant objects verify grant types in advance?


@dataclass(unsafe_hash=True)
class _Grant(ResourceSpec):
    priv: str
    on: ResourceName
    on_type: ResourceType
    to: Role
    grant_option: bool = False
    owner: Role = None
    _privs: list[str] = field(default_factory=list, metadata={"forces_add": True})

    def __post_init__(self):
        super().__post_init__()
        if isinstance(self.priv, str):
            self.priv = self.priv.upper()
        if not self._privs:
            if self.priv == "ALL":
                self._privs = sorted(_all_privs_for_resource_type(self.on_type))
            else:
                self._privs = [self.priv]


class Grant(Resource):
    """
    Description:
        Represents a grant of privileges on a resource to a role in Snowflake.

    Snowflake Docs:
        https://docs.snowflake.com/en/sql-reference/sql/grant-privilege

    Fields:
        priv (string, required): The privilege to grant. Examples include 'SELECT', 'INSERT', 'CREATE TABLE'.
        on (string or Resource, required): The resource on which the privilege is granted. Can be a string like 'ACCOUNT' or a specific resource object.
        to (string or Role, required): The role to which the privileges are granted.
        grant_option (bool): Specifies whether the grantee can grant the privileges to other roles. Defaults to False.
        owner (string or Role): The owner role of the grant. Defaults to 'SYSADMIN'.

    Python:

        ```python
        # Global Privs:
        grant = Grant(priv="CREATE WAREHOUSE", on="ACCOUNT", to="somerole")

        # Warehouse Privs:
        grant = Grant(priv="OPERATE", on=Warehouse(name="foo"), to="somerole")
        grant = Grant(priv="OPERATE", on_warehouse="foo", to="somerole")

        # Schema Privs:
        grant = Grant(priv="CREATE TABLE", on=Schema(name="foo"), to="somerole")
        grant = Grant(priv="CREATE TABLE", on_schema="foo", to="somerole")

        # Table Privs:
        grant = Grant(priv="SELECT", on_table="sometable", to="somerole")
        ```

    Yaml:

        ```yaml
        - Grant:
            priv: "SELECT"
            on: "some_table"
            to: "some_role"
            grant_option: true
        ```
    """

    resource_type = ResourceType.GRANT
    props = Props(
        to=IdentifierProp("to", eq=False, consume="role"),
        grant_option=FlagProp("with grant option"),
    )
    scope = AccountScope()
    spec = _Grant

    def __init__(
        self,
        priv: str = None,
        on: Any = None,
        to: Role = None,
        grant_option: bool = False,
        owner: str = None,
        **kwargs,
    ):

        kwargs.pop("_privs", None)

        priv = priv.value if isinstance(priv, ParseableEnum) else priv

        # Handle instantiation from data dict
        on_type = kwargs.pop("on_type", None)
        if on_type:
            on_type = ResourceType(on_type)

        # Collect on_ kwargs
        on_kwargs = {}
        for keyword, arg in kwargs.copy().items():
            if keyword.startswith("on_"):
                on_kwargs[keyword] = kwargs.pop(keyword)

        # Handle dynamic on_ kwargs
        if on_kwargs:
            for keyword, arg in on_kwargs.items():
                if on is not None:
                    raise ValueError("You can only specify one 'on' parameter, multiple found")
                elif keyword.startswith("on_all"):
                    raise ValueError("You must use GrantOnAll for all grants")
                elif keyword.startswith("on_future"):
                    raise ValueError("You must use FutureGrant for future grants")
                else:
                    # Grant targeting a specific resource
                    # on_{resource} kwargs
                    # on_schema="foo" -> on=Schema(name="foo")
                    on = arg
                    on_type = resource_type_for_label(keyword[3:])
        # Handle on= kwarg
        else:
            if on is None:
                raise ValueError("You must specify an 'on' parameter")
            elif isinstance(on, ResourcePointer):
                on_type = on.resource_type
                on = on.name
            elif isinstance(on, Resource):
                on_type = on.resource_type
                on = on._data.name
            elif isinstance(on, str) and on.upper() == "ACCOUNT":
                on = "ACCOUNT"
                on_type = ResourceType.ACCOUNT

        if owner is None:
            if on_type == ResourceType.ACCOUNT and isinstance(priv, GlobalPriv):
                owner = GLOBAL_PRIV_DEFAULT_OWNERS.get(priv, "SYSADMIN")
            # Hacky fix
            elif on_type == ResourceType.SCHEMA and on.upper().startswith("SNOWFLAKE"):
                owner = "ACCOUNTADMIN"
            elif "INTEGRATION" in str(on_type):
                owner = "ACCOUNTADMIN"
            else:
                owner = "SYSADMIN"

        if to is None and kwargs.get("to_role"):
            to = kwargs.pop("to_role")

        super().__init__(**kwargs)
        self._data: _Grant = _Grant(
            priv=priv,
            on=on,
            on_type=on_type,
            to=to,
            grant_option=grant_option,
            owner=owner,
        )

        self.requires(self._data.to)
        granted_on = None
        if on_type:
            granted_on = ResourcePointer(name=on, resource_type=on_type)
            self.requires(granted_on)

    def __repr__(self):  # pragma: no cover
        priv = getattr(self._data, "priv", "")
        on = getattr(self._data, "on", "")
        to = getattr(self._data, "to", "")
        return f"{self.__class__.__name__}(priv={priv}, on={on}, to={to})"

    @classmethod
    def from_sql(cls, sql):
        parsed = _parse_grant(sql)
        return cls(**parsed)

    @property
    def fqn(self):
        return grant_fqn(self._data)

    @property
    def on(self) -> str:
        return self._data.on

    @property
    def on_type(self) -> ResourceType:
        return self._data.on_type

    @property
    def to(self):
        return self._data.to

    @property
    def priv(self):
        return self._data.priv


def grant_fqn(grant: _Grant):
    on = f"{resource_label_for_type(grant.on_type)}/{grant.on}"
    # if grant.on_type == ResourceType.ACCOUNT:
    #     on = "ACCOUNT"
    return FQN(
        name=grant.to.name,
        params={
            "priv": grant.priv,
            "on": on,
        },
    )


@dataclass(unsafe_hash=True)
class _FutureGrant(ResourceSpec):
    priv: str
    on_type: ResourceType
    in_type: ResourceType
    in_name: ResourceName
    to: Role
    grant_option: bool = False

    def __post_init__(self):
        super().__post_init__()
        if isinstance(self.priv, str):
            self.priv = self.priv.upper()


class FutureGrant(Resource):
    """
    GRANT <privilege> ON FUTURE <on_type> IN <in_type> <in_name> TO <to> [ WITH GRANT OPTION ]
    """

    resource_type = ResourceType.FUTURE_GRANT
    props = Props(
        priv=IdentifierProp("priv", eq=False),
        on_type=IdentifierProp("on type", eq=False),
        database=IdentifierProp("database", eq=False),
        to=IdentifierProp("to", eq=False),
    )
    scope = AccountScope()
    spec = _FutureGrant

    def __init__(
        self,
        priv: str,
        to: Role,
        grant_option: bool = False,
        owner: str = None,
        **kwargs,
    ):
        """
        Usage
        -----

        Database Object Privs:
        >>> Grant(priv="CREATE TABLE", on_future_schemas_in=Database(name="somedb"), to="somerole")
        >>> Grant(priv="CREATE TABLE", on_future_schemas_in_database="somedb", to="somerole")

        Schema Object Privs:
        >>> Grant(priv="SELECT", on_future_tables_in=Schema(name="someschema"), to="somerole")
        >>> Grant(priv="READ", on_future_image_repositories_in_schema="someschema", to="somerole")

        """
        on_type = kwargs.pop("on_type", None)
        in_type = kwargs.pop("in_type", None)
        in_name = kwargs.pop("in_name", None)
        granted_in_ref = None

        if all([on_type, in_type, in_name]):
            in_type = ResourceType(in_type)
            on_type = ResourceType(on_type)
            granted_in_ref = ResourcePointer(name=in_name, resource_type=in_type)

        else:

            # Collect on_ kwargs
            on_kwargs = {}
            for keyword, _ in kwargs.copy().items():
                if keyword.startswith("on_future_"):
                    on_kwargs[keyword] = kwargs.pop(keyword)

            if len(on_kwargs) != 1:
                raise ValueError("You must specify one 'on_future_' parameter")

            # Handle on_future_ kwargs
            if on_kwargs:
                for keyword, arg in on_kwargs.items():

                    # At some point we need to support _in_sometype=SomeType(blah)

                    if isinstance(arg, Resource):
                        on_type = ResourceType(singularize(keyword[10:-3]))
                        in_type = arg.resource_type
                        in_name = str(arg.fqn)
                        granted_in_ref = arg
                    else:
                        on_stmt, in_stmt = keyword.split("_in_")
                        on_type = ResourceType(singularize(on_stmt[10:]))
                        in_type = ResourceType(in_stmt)
                        in_name = arg
                        granted_in_ref = ResourcePointer(name=in_name, resource_type=in_type)

        super().__init__(**kwargs)
        self._data: _FutureGrant = _FutureGrant(
            priv=priv,
            on_type=on_type,
            in_type=in_type,
            in_name=in_name,
            to=to,
            grant_option=grant_option,
        )
        self.requires(self._data.to)
        if granted_in_ref:
            self.requires(granted_in_ref)

    @classmethod
    def from_sql(cls, sql):
        parsed = _parse_grant(sql)
        return cls(**parsed)

    @property
    def fqn(self):
        return future_grant_fqn(self._data)

    @property
    def priv(self) -> str:
        return self._data.priv

    @property
    def on_type(self) -> ResourceType:
        return self._data.on_type

    @property
    def in_type(self) -> ResourceType:
        return self._data.in_type

    @property
    def in_name(self) -> str:
        return self._data.in_name

    @property
    def to(self):
        return self._data.to


def future_grant_fqn(data: _FutureGrant):
    in_type = resource_label_for_type(data.in_type)
    in_name = data.in_name
    on_type = resource_label_for_type(data.on_type).upper()
    return FQN(
        name=data.to.name,
        params={
            "priv": data.priv,
            "on": f"{in_type}/{in_name}.<{on_type}>",
        },
    )


@dataclass(unsafe_hash=True)
class _GrantOnAll(ResourceSpec):
    priv: str
    on_type: ResourceType
    in_type: ResourceType
    in_name: str
    to: Role
    grant_option: bool = False
    owner: Role = "SECURITYADMIN"

    def __post_init__(self):
        super().__post_init__()
        if isinstance(self.priv, str):
            self.priv = self.priv.upper()
        if self.in_type not in [ResourceType.DATABASE, ResourceType.SCHEMA]:
            raise ValueError("in_type must be either DATABASE or SCHEMA")


class GrantOnAll(Resource):
    """
    GRANT
          { schemaPrivileges         | ALL [ PRIVILEGES ] } ON ALL SCHEMAS IN DATABASE <db_name>
        | { schemaObjectPrivileges   | ALL [ PRIVILEGES ] } ON ALL <object_type_plural> IN { DATABASE <db_name> | SCHEMA <schema_name> }
        }
    TO [ ROLE ] <role_name> [ WITH GRANT OPTION ]
    """

    resource_type = ResourceType.GRANT_ON_ALL
    props = Props(
        priv=IdentifierProp("priv", eq=False),
        on_type=IdentifierProp("on type", eq=False),
        in_type=IdentifierProp("in type", eq=False),
        in_name=IdentifierProp("in name", eq=False),
        to=IdentifierProp("to", eq=False),
        grant_option=FlagProp("with grant option"),
    )
    scope = AccountScope()
    spec = _GrantOnAll

    def __init__(
        self,
        priv: str,
        to: Role,
        grant_option: bool = False,
        **kwargs,
    ):
        """
        Usage
        -----

        Schema Privs:
        >>> GrantOnAll(priv="CREATE TABLE", on_all_schemas_in_database="somedb", to="somerole")
        >>> GrantOnAll(priv="CREATE VIEW", on_all_schemas_in=Database(name="somedb"), to="somerole")

        Schema Object Privs:
        >>> GrantOnAll(priv="SELECT", on_all_tables_in_schema="sch", to="somerole")
        >>> GrantOnAll(priv="SELECT", on_all_views_in_database="somedb", to="somerole")

        """
        on_type = kwargs.pop("on_type", None)
        in_type = kwargs.pop("in_type", None)
        in_name = kwargs.pop("in_name", None)

        # Init from serialized
        if all([on_type, in_type, in_name]):
            in_type = ResourceType(in_type)
            on_type = ResourceType(on_type)
        else:

            # Collect on_ kwargs
            on_kwargs = {}
            for keyword, _ in kwargs.copy().items():
                if keyword.startswith("on_all_"):
                    on_kwargs[keyword] = kwargs.pop(keyword)

            if len(on_kwargs) != 1:
                raise ValueError("You must specify one 'on_all_' parameter")

            # Handle on_all_ kwargs
            if on_kwargs:
                for keyword, arg in on_kwargs.items():
                    on_keyword = keyword.split("_")[2]
                    on_type = ResourceType(singularize(on_keyword))
                    if isinstance(arg, Resource):
                        in_type = arg.resource_type
                        in_name = str(arg.fqn)
                    else:
                        in_stmt = keyword.split("_in_")[1]
                        in_type = ResourceType(in_stmt)
                        in_name = arg

        if in_type == ResourceType.SCHEMA and in_name.upper().startswith("SNOWFLAKE"):
            owner = "ACCOUNTADMIN"
        else:
            owner = "SECURITYADMIN"

        super().__init__(**kwargs)
        self._data: _GrantOnAll = _GrantOnAll(
            priv=priv,
            on_type=on_type,
            in_type=in_type,
            in_name=in_name,
            to=to,
            grant_option=grant_option,
            owner=owner,
        )
        self.requires(self._data.to)

    @classmethod
    def from_sql(cls, sql):
        parsed = _parse_grant(sql)
        return cls(**parsed)

    @property
    def fqn(self):
        return grant_on_all_fqn(self._data)


def grant_on_all_fqn(grant: _GrantOnAll):
    return FQN(
        name=grant.to.name,
        params={
            "on_type": str(grant.on_type),
            "in_type": str(grant.in_type),
            "in_name": grant.in_name,
        },
    )


@dataclass(unsafe_hash=True)
class _RoleGrant(ResourceSpec):
    role: Role
    to_role: Role = None
    to_user: User = None
    # owner: str = "SECURITYADMIN"

    def __post_init__(self):
        super().__post_init__()
        if self.to_role is not None and self.to_user is not None:
            raise ValueError("You can only grant to a role or a user, not both")
        if self.to_role is None and self.to_user is None:
            raise ValueError("You must specify a role or a user to grant to")


class RoleGrant(Resource):
    """
    GRANT ROLE <name> TO { ROLE <parent_role_name> | USER <user_name> }
    """

    resource_type = ResourceType.ROLE_GRANT
    props = Props(
        role=IdentifierProp("role", eq=False),
        to_role=IdentifierProp("to role", eq=False),
        to_user=IdentifierProp("to user", eq=False),
    )
    scope = AccountScope()
    spec = _RoleGrant

    def __init__(
        self,
        role: Role,
        to_role: Role = None,
        to_user: User = None,
        # owner: str = None,  # = "SECURITYADMIN"
        **kwargs,
    ):
        """
        Usage
        -----

        Grant to Role:
        >>> RoleGrant(role="somerole", to_role="someotherrole")
        >>> RoleGrant(role="somerole", to=Role(name="someuser"))

        Grant to User:
        >>> RoleGrant(role="somerole", to_user="someuser")
        >>> RoleGrant(role="somerole", to=User(name="someuser"))
        """

        to = kwargs.pop("to", None)
        if to:
            if to_role or to_user:
                raise ValueError("You can only grant to a role or a user, not both")
            if isinstance(to, Role):
                to_role = to
            elif isinstance(to, User):
                to_user = to
            else:
                raise ValueError("You can only grant to a role or a user")

        super().__init__(**kwargs)
        self._data: _RoleGrant = _RoleGrant(
            role=role,
            to_role=to_role,
            to_user=to_user,
            # owner=owner,
        )
        self.requires(
            self._data.role,
            self._data.to_role,
            self._data.to_user,
        )

    @classmethod
    def from_sql(cls, sql):
        props = _parse_grant(sql)
        return RoleGrant(**props)

    def __repr__(self):  # pragma: no cover
        role = getattr(self._data, "role", "")
        to_role = getattr(self._data, "to_role", "")
        to_user = getattr(self._data, "to_user", "")
        to = to_role or to_user
        return f"RoleGrant(role={role}, to={to})"

    # @property
    # def fqn(self):
    #     subject = "user" if self._data.to_user else "role"
    #     name = self._data.to_user.name if self._data.to_user else self._data.to_role.name
    #     return FQN(
    #         name=self._data.role.name,
    #         params={subject: name},
    #     )

    @property
    def fqn(self):
        return role_grant_fqn(self._data)

    @property
    def role(self) -> Role:
        return self._data.role

    @property
    def to(self) -> Union[Role, User]:
        return self._data.to_role or self._data.to_user


def role_grant_fqn(role_grant: _RoleGrant):
    subject = "user" if role_grant.to_user else "role"
    name = role_grant.to_user.name if role_grant.to_user else role_grant.to_role.name
    return FQN(
        name=role_grant.role.name,
        params={subject: name},
    )
