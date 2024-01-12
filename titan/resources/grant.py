# from typing_extensions import Annotated

# from pydantic import BeforeValidator, Field, PlainSerializer, model_validator

# from .base import Resource, AccountScoped, T_Schema, _fix_class_documentation
# from .validators import coerce_from_str, serialize_as_named_resource
# from ..builder import SQL, tidy_sql
# from ..enums import ParseableEnum
# from ..helpers import listify

# from ..parse import _parse_grant, _parse_props
# from ..privs import Privs, GLOBAL_PRIV_DEFAULT_OWNERS
# from ..props import Props, IdentifierProp, FlagProp


from dataclasses import dataclass
from typing import Any

from .resource import Resource, ResourceSpec
from .role import Role
from .user import User
from ..enums import ParseableEnum, ResourceType
from ..identifiers import FQN
from ..parse import _parse_grant
from ..privs import GlobalPriv, GLOBAL_PRIV_DEFAULT_OWNERS
from ..props import Props
from ..scope import AccountScope


@dataclass
class _Grant(ResourceSpec):
    priv: str
    on: Any = None
    on_all: Any = None
    on_future: Any = None
    on_scope: str = None
    to: str = None
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
    props = Props()
    scope = AccountScope()
    spec = _Grant

    def __init__(
        self,
        priv: str = None,
        on: Any = None,
        to: str = None,
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
        >>> Grant(priv="CREATE TABLE", on_all_schemas_in_database="somedb", to="somerole")
        >>> Grant(priv="CREATE TABLE", on_future_schemas_in=Database(name="somedb"), to="somerole")

        Table Privs:
        >>> Grant(priv="SELECT", on_all_tables_in_schema="sch", to="somerole")
        >>> Grant(priv="SELECT", on_future_tables_in_schema="sch", to="somerole")
        >>> Grant(priv="SELECT", on_future_tables_in=Database(name="somedb"), to="somerole")

        """
        on_all = None
        on_future = None

        # Collect on_ kwargs
        on_kwargs = {}
        for keyword, arg in kwargs.copy().items():
            if keyword.startswith("on_"):
                on_kwargs[keyword] = kwargs.pop(keyword)

        for keyword, arg in on_kwargs.items():
            if on is not None:
                raise ValueError("You can only specify one 'on' parameter, multiple found")

            # Ex: on_future_schemas_in_database -> on_future_schemas_in, database
            is_scoped_grant = "_in_" in keyword or keyword.endswith("_in")

            if is_scoped_grant:
                if keyword.endswith("_in"):
                    keyword = keyword[:-3]
                    on = arg
                elif "_in_" in keyword:
                    keyword, resource_type = keyword.split("_in_")
                    on = f"{resource_type} {arg}"
                    # resource_cls = Resource.classes[resource_key]
                    # on = resource_cls(name=arg, stub=True)

                if keyword.startswith("on_all"):
                    on_all = keyword[7:].replace("_", " ").upper()
                elif keyword.startswith("on_future"):
                    on_future = keyword[10:].replace("_", " ").upper()
            else:
                # Grant targeting a specific resource
                # on_{resource} kwargs
                # on_schema="foo" -> on=Schema(name="foo", stub=True)
                # TODO: find a different way to create a reference pointer to the ON resource
                on = f"{keyword[3:]} {arg}"

        if owner is None:
            if on == "ACCOUNT" and isinstance(priv, GlobalPriv):
                owner = GLOBAL_PRIV_DEFAULT_OWNERS.get(priv, "SYSADMIN")
            else:
                owner = "SYSADMIN"

        super().__init__(**kwargs)
        self._data: _Grant = _Grant(
            priv=priv,
            on=on,
            on_all=on_all,
            on_future=on_future,
            to=to,
            grant_option=grant_option,
            owner=owner,
        )

    def __repr__(self):
        priv = getattr(self, "priv", "")
        on = getattr(self, "on", "")
        to = getattr(self, "to", "")
        return f"{self.__class__.__name__}(priv={priv}, on={on}, to={to})"

    @classmethod
    def from_sql(cls, sql):
        # parsed = _parse_grant(sql)
        # return cls(**parsed)
        raise NotImplementedError

    @property
    def fqn(self):
        to = self._data.to.name if isinstance(self._data.to, Resource) else self._data.to
        return FQN(name=to, params={"on": self._data.on})

    @property
    def name(self):
        priv = self.priv if isinstance(self.priv, str) else self.priv.value
        if " " in priv:
            priv = f'"{priv}"'
        return priv

    @property
    def on(self):
        return self._data.on

    @property
    def on_all(self):
        return self._data.on_all

    @property
    def on_future(self):
        return self._data.on_future

    @property
    def to(self):
        return self._data.to

    @property
    def priv(self):
        return self._data.priv


@dataclass
class _RoleGrant(ResourceSpec):
    role: Role
    to_role: Role = None
    to_user: User = None
    owner: str = "USERADMIN"

    def __post_init__(self):
        super().__post_init__()
        if self.to_role is not None and self.to_user is not None:
            raise ValueError("You can only grant to a role or a user, not both")


class RoleGrant(Resource):
    """
    GRANT ROLE <name> TO { ROLE <parent_role_name> | USER <user_name> }
    """

    resource_type = "GRANT"
    props = Props()
    scope = AccountScope()
    spec = _RoleGrant

    def __init__(
        self,
        role: str,
        to_role: str = None,
        to_user: str = None,
        owner: str = "USERADMIN",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._data: _RoleGrant = _RoleGrant(
            role=role,
            to_role=to_role,
            to_user=to_user,
            owner=owner,
        )

    @classmethod
    def from_sql(cls, sql):
        # props = _parse_props(cls.props, sql)
        # return RoleGrant(**props)
        raise NotImplementedError

    @property
    def fully_qualified_name(self):
        subject = "user" if self._data.to_user else "role"
        name = self._data.to_user.name if self._data.to_user else self._data.to_role.name
        return FQN(name=self.name, params={subject: name})

    @property
    def name(self):
        return self._data.role.name
