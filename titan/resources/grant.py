from typing import List, Union
from typing_extensions import Annotated

from pydantic import BeforeValidator, Field, PlainSerializer, model_validator

from .base import Resource, AccountScoped, Database, Schema, serialize_resource_by_name
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


# class Grant(Resource, AccountScoped):
#     resource_type = "GRANT"
#     lifecycle_privs = Privs(
#         create=GlobalPriv.MANAGE_GRANTS,
#         delete=GlobalPriv.MANAGE_GRANTS,
#     )

#     # def __new__(cls, **kwargs):
#     #     print("ok")
#     # file_type = FileType.parse(type)
#     # file_type_cls = FileTypeMap[file_type]
#     # return file_type_cls(type=file_type, **kwargs)

# @classmethod
# def from_sql(cls, sql):
#     parsed = _parse_grant(sql)
#     grant_cls = Resource.classes[parsed["resource_key"]]

#     # RoleGrants
#     if grant_cls is RoleGrant:
#         props = _parse_props(RoleGrant.props, sql)
#         return RoleGrant(**props)

#     props = _parse_props(grant_cls.props, parsed["remainder"])
#     return grant_cls(privs=parsed["privs"], on=parsed["on"], **props)


class Grant(Resource, AccountScoped):
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

    priv: Annotated[ParseableEnum, PlainSerializer(lambda en: en.value if en else None)] = None

    # TODO: This should probably some new annotated type like NamedResource
    # on: Annotated[Resource, serialize_resource_by_name]
    on: Annotated[str, BeforeValidator(serialize_as_named_resource)] = None
    to: T_Role = None
    grant_option: bool = False
    owner: str = None

    def model_post_init(self, ctx):
        super().model_post_init(ctx)
        if self.owner is None:
            self.owner = GLOBAL_PRIV_DEFAULT_OWNERS.get(self.priv, "SYSADMIN")

    @classmethod
    def from_sql(cls, sql):
        parsed = _parse_grant(sql)
        raise NotImplementedError("TODO: implement Grant.from_sql")
        # props = _parse_props(cls.props, parsed["remainder"])
        # return cls(privs=parsed["privs"], on=parsed["on"], **props)

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
            # "TO",
            # data["to"],
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
        return self.lifecycle_create(self.fqn, data)


# class OwnershipGrant(Grant):
#     """
#     -- Role
#     GRANT OWNERSHIP
#     { ON { <object_type> <object_name> | ALL <object_type_plural> IN { DATABASE <db_name> | SCHEMA <schema_name> } }
#     | ON FUTURE <object_type_plural> IN { DATABASE <db_name> | SCHEMA <schema_name> }
#     }
#     TO ROLE <role_name>
#     [ { REVOKE | COPY } CURRENT GRANTS ]

#     -- Database role
#     GRANT OWNERSHIP
#     { ON { <object_type> <object_name> | ALL <object_type_plural> IN { DATABASE <db_name> | SCHEMA <schema_name> } }
#     | ON FUTURE <object_type_plural> IN { DATABASE <db_name> | SCHEMA <schema_name> }
#     }
#     TO DATABASE ROLE <database_role_name>
#     [ { REVOKE | COPY } CURRENT GRANTS ]
#     """

#     props = Props(
#         to=IdentifierProp("to", eq=False, consume="role"),
#     )

#     on: Annotated[Resource, serialize_resource_by_name]
#     to: T_Role


# class AccountObjectGrant(PrivGrant):
#     """
#     GRANT { accountObjectPrivileges | ALL [ PRIVILEGES ] }
#     ON { USER | RESOURCE MONITOR | WAREHOUSE | DATABASE | INTEGRATION | FAILOVER GROUP | REPLICATION GROUP } <object_name>
#     TO [ ROLE ] <role_name>
#     [ WITH GRANT OPTION ]
#     """

#     privs: list


# class SchemaGrant(PrivGrant):
#     """
#     GRANT { schemaPrivileges | ALL [ PRIVILEGES ] }
#     ON SCHEMA <schema_name>
#     TO [ ROLE ] <role_name>
#     [ WITH GRANT OPTION ]
#     """

#     privs: Annotated[List[SchemaPriv], BeforeValidator(listify)]
#     on: Annotated[Schema, BeforeValidator(coerce_from_str(Schema))]


# class SchemasGrant(PrivGrant):
#     """
#     GRANT { schemaPrivileges | ALL [ PRIVILEGES ] }
#     ON ALL SCHEMAS IN DATABASE <db_name>
#     TO [ ROLE ] <role_name>
#     [ WITH GRANT OPTION ]
#     """

#     privs: Annotated[List[SchemaPriv], BeforeValidator(listify)]
#     on: Annotated[Database, BeforeValidator(coerce_from_str(Database))]


# class FutureSchemasGrant(PrivGrant):
#     """
#     GRANT { schemaPrivileges | ALL [ PRIVILEGES ] }
#     ON FUTURE SCHEMAS IN DATABASE <db_name>
#     TO [ ROLE ] <role_name>
#     [ WITH GRANT OPTION ]
#     """

#     privs: Annotated[List[SchemaPriv], BeforeValidator(listify)]
#     on: Annotated[Database, BeforeValidator(coerce_from_str(Database))]


# class SchemaObjectGrant(PrivGrant):
#     """
#     GRANT { schemaObjectPrivileges | ALL [ PRIVILEGES ] }
#     ON <object_type> <object_name>
#     TO [ ROLE ] <role_name>
#     [ WITH GRANT OPTION ]
#     """

#     privs: list


# class SchemaObjectsGrant(PrivGrant):
#     """
#     GRANT { schemaObjectPrivileges | ALL [ PRIVILEGES ] }
#     ON ALL <object_type>
#     IN { DATABASE <db_name> | SCHEMA <schema_name> }
#     TO [ ROLE ] <role_name>
#     [ WITH GRANT OPTION ]
#     """

#     privs: list


# class FutureSchemaObjectsGrant(PrivGrant):
#     """
#     GRANT { schemaObjectPrivileges | ALL [ PRIVILEGES ] }
#     ON FUTURE <object_type>
#     IN { DATABASE <db_name> | SCHEMA <schema_name> }
#     TO [ ROLE ] <role_name>
#     [ WITH GRANT OPTION ]
#     """

#     privs: list


class RoleGrant(Resource, AccountScoped):
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
        # Deprecated
        # urn:XY54321:role_grant/CI?user=SYSADMIN
        # role = self.role.name
        # param = "user" if self.to_user else "role"
        # value = self.to_user.name if self.to_user else self.to_role.name
        # return f"{role}?{param}={value}"
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
