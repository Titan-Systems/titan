from typing import Any, List, Union, Type
from typing_extensions import Annotated

from pydantic import BeforeValidator, Field, PlainSerializer, model_validator

from .base import Resource, AccountScoped, T_Schema, _fix_class_documentation
from .role import T_Role
from .user import T_User
from .validators import coerce_from_str, serialize_as_named_resource
from ..builder import SQL, tidy_sql
from ..enums import ParseableEnum
from ..helpers import listify
from ..identifiers import FQN
from ..parse import _parse_grant, _parse_props
from ..privs import Privs, GLOBAL_PRIV_DEFAULT_OWNERS
from ..props import Props, IdentifierProp, FlagProp
from ..privs import GlobalPriv, SchemaPriv


@_fix_class_documentation
class Grant(AccountScoped, Resource):
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

    resource_type = "GRANT"
    serialize_as_list = True
    props = Props(
        to=IdentifierProp("to", eq=False, consume="role"),
        grant_option=FlagProp("with grant option"),
    )

    priv: str = None
    on: Any = None
    on_all: Any = None
    on_future: Any = None
    on_scope: str = None
    to: T_Role = None
    grant_option: bool = False
    owner: str = None

    def __init__(
        self,
        priv: str = None,
        on: Any = None,
        to: T_Role = None,
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

        for keyword, arg in kwargs.items():
            if keyword.startswith("on_"):
                if on is not None:
                    raise ValueError("You can only specify one 'on' parameter, multiple found")

                is_scoped_grant = "_in_" in keyword or keyword.endswith("_in")

                if is_scoped_grant:
                    if keyword.endswith("_in"):
                        keyword = keyword[:-3]
                        on = arg
                    elif "_in_" in keyword:
                        keyword, resource_key = keyword.split("_in_")
                        resource_cls = Resource.classes[resource_key]
                        on = resource_cls(name=arg, stub=True)

                    if keyword.startswith("on_all"):
                        on_all = keyword[7:].replace("_", " ").upper()
                    elif keyword.startswith("on_future"):
                        on_future = keyword[10:].replace("_", " ").upper()
                else:
                    # Grant targeting a specific resource
                    # on_{resource} kwargs
                    # on_schema="foo" -> on=Schema(name="foo", stub=True)
                    resource_cls = Resource.classes[keyword[3:]]
                    resource_name = arg
                    on = resource_cls(name=resource_name, stub=True)

        if owner is None and on == "ACCOUNT" and isinstance(priv, GlobalPriv):
            owner = GLOBAL_PRIV_DEFAULT_OWNERS.get(priv, "SYSADMIN")

        super().__init__(
            priv=priv.value if isinstance(priv, ParseableEnum) else priv,
            on=on,
            on_all=on_all,
            on_future=on_future,
            to=to,
            grant_option=grant_option,
            owner=owner or "SYSADMIN",
        )

    @classmethod
    def from_sql(cls, sql):
        parsed = _parse_grant(sql)
        return cls(**parsed)

    @property
    def name(self):
        priv = self.priv if isinstance(self.priv, str) else self.priv.value
        if " " in priv:
            priv = f'"{priv}"'
        return priv

    @property
    def fully_qualified_name(self):
        return FQN(name=self.to.name, params={"on": self.on})

    @classmethod
    def lifecycle_create(cls, fqn: FQN, data):
        return SQL(
            "GRANT",
            data["priv"],
            "ON",
            data["on"],
            cls.props.render(data),
            _use_role=data["owner"],
        )

    @classmethod
    def lifecycle_delete(cls, fqn: FQN, data, cascade=False):
        return SQL(
            "REVOKE",
            data["priv"],
            "ON",
            data["on"],
            "FROM",
            data["to"],
            "CASCADE" if cascade else "RESTRICT",
            _use_role=data["owner"],
        )

    def create_sql(self):
        data = self.model_dump(exclude_none=True, exclude_defaults=True)
        return str(self.lifecycle_create(self.fqn, data))


@_fix_class_documentation
class RoleGrant(AccountScoped, Resource):
    """
    GRANT ROLE <name> TO { ROLE <parent_role_name> | USER <user_name> }
    """

    resource_type = "GRANT"
    props = Props(
        _start_token="grant",
        role=IdentifierProp("role", eq=False),
        to_role=IdentifierProp("to role", eq=False),
        to_user=IdentifierProp("to user", eq=False),
    )

    role: T_Role
    to_role: T_Role = None
    to_user: T_User = None
    owner: str = "SYSADMIN"

    @model_validator(mode="after")
    def ensure_single_target_type(self) -> "RoleGrant":
        if self.to_role is not None and self.to_user is not None:
            raise ValueError("You can only grant to a role or a user, not both")
        return self

    @classmethod
    def from_sql(cls, sql):
        props = _parse_props(cls.props, sql)
        return RoleGrant(**props)

    @property
    def fully_qualified_name(self):
        subject = "user" if self.to_user else "role"
        name = self.to_user.name if self.to_user else self.to_role.name
        return FQN(name=self.name, params={subject: name})

    @property
    def name(self):
        return self.role.name

    @classmethod
    def lifecycle_create(cls, fqn, data):
        return tidy_sql("GRANT", cls.props.render(data))

    @classmethod
    def lifecycle_delete(cls, fqn, data):
        return tidy_sql(
            "REVOKE ROLE",
            data["role"],
            "FROM",
            "ROLE" if data.get("to_role") else "USER",
            data["to_role"] if data.get("to_role") else data["to_user"],
        )
