from dataclasses import dataclass
from typing import Any

from inflection import singularize

from .resource import Resource, ResourcePointer, ResourceSpec
from .role import Role
from .user import User
from ..enums import ResourceType
from ..identifiers import FQN, resource_label_for_type, resource_type_for_label
from ..parse import _parse_grant, _parse_props
from ..privs import GlobalPriv, GLOBAL_PRIV_DEFAULT_OWNERS
from ..props import Props, FlagProp, IdentifierProp
from ..scope import AccountScope


@dataclass(unsafe_hash=True)
class _Grant(ResourceSpec):
    priv: str
    on: str
    on_type: ResourceType
    to: Role
    grant_option: bool = False
    owner: str = None


class Grant(Resource):
    """
    GRANT {  { globalPrivileges         | ALL [ PRIVILEGES ] } ON ACCOUNT
        | { accountObjectPrivileges  | ALL [ PRIVILEGES ] } ON { USER | RESOURCE MONITOR | WAREHOUSE | DATABASE | INTEGRATION | FAILOVER GROUP | REPLICATION GROUP } <object_name>
        | { schemaPrivileges         | ALL [ PRIVILEGES ] } ON { SCHEMA <schema_name> | ALL SCHEMAS IN DATABASE <db_name> }
        | { schemaPrivileges         | ALL [ PRIVILEGES ] } ON { FUTURE SCHEMAS IN DATABASE <db_name> }
        | { schemaObjectPrivileges   | ALL [ PRIVILEGES ] } ON { <object_type> <object_name> | ALL <object_type_plural> IN { DATABASE <db_name> | SCHEMA <schema_name> } }
        | { schemaObjectPrivileges   | ALL [ PRIVILEGES ] } ON FUTURE <object_type_plural> IN { DATABASE <db_name> | SCHEMA <schema_name> }
        }
    TO [ ROLE ] <role_name> [ WITH GRANT OPTION ]

    globalPrivileges ::=
        {
            CREATE {
                    ACCOUNT | DATA EXCHANGE LISTING | DATABASE | FAILOVER GROUP | INTEGRATION
                    | NETWORK POLICY | REPLICATION GROUP | ROLE | SHARE | USER | WAREHOUSE
            }
            | APPLY { { MASKING | PASSWORD | ROW ACCESS | SESSION } POLICY | TAG }
            | ATTACH POLICY | AUDIT |
            | EXECUTE { ALERT | TASK }
            | IMPORT SHARE
            | MANAGE GRANTS
            | MODIFY { LOG LEVEL | TRACE LEVEL | SESSION LOG LEVEL | SESSION TRACE LEVEL }
            | MONITOR { EXECUTION | SECURITY | USAGE }
            | OVERRIDE SHARE RESTRICTIONS | RESOLVE ALL
        }
        [ , ... ]

    accountObjectPrivileges ::=
        -- For DATABASE
            { CREATE { DATABASE ROLE | SCHEMA } | IMPORTED PRIVILEGES | MODIFY | MONITOR | USAGE } [ , ... ]
        -- For FAILOVER GROUP
            { FAILOVER | MODIFY | MONITOR | REPLICATE } [ , ... ]
        -- For INTEGRATION
            { USAGE | USE_ANY_ROLE } [ , ... ]
        -- For REPLICATION GROUP
            { MODIFY | MONITOR | REPLICATE } [ , ... ]
        -- For RESOURCE MONITOR
            { MODIFY | MONITOR } [ , ... ]
        -- For USER
            { MONITOR } [ , ... ]
        -- For WAREHOUSE
            { MODIFY | MONITOR | USAGE | OPERATE } [ , ... ]

    schemaPrivileges ::=
        ADD SEARCH OPTIMIZATION
        | CREATE {
            ALERT | EXTERNAL TABLE | FILE FORMAT | FUNCTION
            | MATERIALIZED VIEW | PIPE | PROCEDURE
            | { MASKING | PASSWORD | ROW ACCESS | SESSION } POLICY
            | SECRET | SEQUENCE | STAGE | STREAM
            | TAG | TABLE | TASK | VIEW
            }
        | MODIFY | MONITOR | USAGE
        [ , ... ]

    schemaObjectPrivileges ::=
        -- For ALERT
            OPERATE [ , ... ]
        -- For EVENT TABLE
            { SELECT | INSERT } [ , ... ]
        -- For FILE FORMAT, FUNCTION (UDF or external function), PROCEDURE, SECRET, or SEQUENCE
            USAGE [ , ... ]
        -- For PIPE
            { MONITOR | OPERATE } [ , ... ]
        -- For { MASKING | PASSWORD | ROW ACCESS | SESSION } POLICY or TAG
            APPLY [ , ... ]
        -- For external STAGE
            USAGE [ , ... ]
        -- For internal STAGE
            READ [ , WRITE ] [ , ... ]
        -- For STREAM
            SELECT [ , ... ]
        -- For TABLE
            { SELECT | INSERT | UPDATE | DELETE | TRUNCATE | REFERENCES } [ , ... ]
        -- For TASK
            { MONITOR | OPERATE } [ , ... ]
        -- For VIEW or MATERIALIZED VIEW
            { SELECT | REFERENCES } [ , ... ]
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
        """
        Usage
        -----

        Global Privs:
        >>> Grant(priv="ALL", on="ACCOUNT", to="somerole")

        Warehouse Privs:
        >>> Grant(priv="OPERATE", on=Warehouse(name="foo"), to="somerole")
        >>> Grant(priv="OPERATE", on_warehouse="foo", to="somerole")

        Schema Privs:
        >>> Grant(priv="CREATE TABLE", on=Schema(name="foo"), to="somerole")
        >>> Grant(priv="CREATE TABLE", on_schema="foo", to="somerole")

        Table Privs:
        >>> Grant(priv="SELECT", on_table="sometable", to="somerole")

        """
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

                # Ex: on_future_schemas_in_database -> on_future_schemas_in, database
                is_scoped_grant = "_in_" in keyword or keyword.endswith("_in")

                if is_scoped_grant:

                    if keyword.startswith("on_all"):
                        raise ValueError("You must use GrantOnAll for all grants")
                    elif keyword.startswith("on_future"):
                        raise ValueError("You must use FutureGrant for future grants")
                    else:
                        raise Exception(f"Invalid grant type: {keyword}")
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
            if isinstance(on, Resource):
                on_type = on.resource_type
                on = on._data.name

        if owner is None:
            if on == "ACCOUNT" and isinstance(priv, GlobalPriv):
                owner = GLOBAL_PRIV_DEFAULT_OWNERS.get(priv, "SYSADMIN")
            # Hacky fix
            elif on_type == ResourceType.SCHEMA and on.upper().startswith("SNOWFLAKE"):
                owner = "ACCOUNTADMIN"
            elif "INTEGRATION" in str(on_type):
                owner = "ACCOUNTADMIN"
            else:
                owner = "SYSADMIN"

        super().__init__(**kwargs)
        self._data: _Grant = _Grant(
            priv=priv,
            on=on.upper(),
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
    def name(self):
        priv = self.priv if isinstance(self.priv, str) else self.priv.value
        if " " in priv:
            priv = f'"{priv}"'
        return priv

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
    return FQN(name=grant.to.name, params={"on": grant.on, "on_type": str(grant.on_type)})


@dataclass(unsafe_hash=True)
class _FutureGrant(ResourceSpec):
    priv: str
    on_type: ResourceType
    in_type: ResourceType
    in_name: str
    to: Role
    grant_option: bool = False
    owner: str = "SECURITYADMIN"


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

        if all([on_type, in_type, in_name]):
            in_type = ResourceType(in_type)
            on_type = ResourceType(on_type)

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
                    else:
                        on_stmt, in_stmt = keyword.split("_in_")
                        on_type = ResourceType(singularize(on_stmt[10:]))
                        in_type = ResourceType(in_stmt)
                        in_name = arg

        if in_type == ResourceType.SCHEMA and in_name.upper().startswith("SNOWFLAKE"):
            owner = "ACCOUNTADMIN"
        else:
            owner = "SECURITYADMIN"

        super().__init__(**kwargs)
        self._data: _FutureGrant = _FutureGrant(
            priv=priv,
            on_type=on_type,
            in_type=in_type,
            in_name=in_name.upper(),
            to=to,
            grant_option=grant_option,
            owner=owner,
        )
        granted_in = ResourcePointer(name=in_name, resource_type=in_type)
        self.requires(granted_in, self._data.to)

    @classmethod
    def from_sql(cls, sql):
        parsed = _parse_grant(sql)
        return cls(**parsed)

    @property
    def fqn(self):
        return future_grant_fqn(self._data)


def future_grant_fqn(grant: _FutureGrant):
    return FQN(
        name=grant.to.name,
        params={"in": f"{resource_label_for_type(grant.in_type)}/{grant.in_name}"},
    )


@dataclass(unsafe_hash=True)
class _GrantOnAll(ResourceSpec):
    priv: str
    on_type: ResourceType
    in_type: ResourceType
    in_name: str
    to: Role
    grant_option: bool = False
    owner: str = "SECURITYADMIN"

    def __post_init__(self):
        super().__post_init__()
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
    owner: str = "SECURITYADMIN"

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

    @property
    def fqn(self):
        subject = "user" if self._data.to_user else "role"
        name = self._data.to_user.name if self._data.to_user else self._data.to_role.name
        return FQN(name=self._data.role.name, params={subject: name})
