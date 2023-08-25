from typing import List, Union
from typing_extensions import Annotated

from pydantic import AfterValidator, BeforeValidator, model_validator, BaseModel

from .base import Resource, AccountScoped, Database, Schema, serialize_resource_by_name
from .role import T_Role
from .user import T_User
from .validators import coerce_from_str, listify
from ..builder import tidy_sql
from ..identifiers import FQN
from ..parse import _parse_grant, _parse_props
from ..privs import Privs
from ..props import Props, IdentifierProp, FlagProp
from ..enums import GlobalPriv, SchemaPriv  # SchemaObjectPrivs, AccountObjectPrivs


class Grant(Resource, AccountScoped):
    resource_type = "GRANT"
    lifecycle_privs = Privs(
        create=GlobalPriv.MANAGE_GRANTS,
        delete=GlobalPriv.MANAGE_GRANTS,
    )

    # def __new__(cls, **kwargs):
    #     print("ok")
    # file_type = FileType.parse(type)
    # file_type_cls = FileTypeMap[file_type]
    # return file_type_cls(type=file_type, **kwargs)

    @classmethod
    def from_sql(cls, sql):
        parsed = _parse_grant(sql)
        grant_cls = Resource.classes[parsed["resource_key"]]

        # RoleGrants
        if grant_cls is RoleGrant:
            props = _parse_props(RoleGrant.props, sql)
            return RoleGrant(**props)

        props = _parse_props(grant_cls.props, parsed["remainder"])
        return grant_cls(privs=parsed["privs"], on=parsed["on"], **props)


class PrivGrant(Grant):
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

    props = Props(
        to=IdentifierProp("to", eq=False, consume="role"),
        with_grant_option=FlagProp("with grant option"),
    )

    privs: Annotated[list, BeforeValidator(listify)]
    on: Annotated[Resource, serialize_resource_by_name]
    to: T_Role
    with_grant_option: bool = None

    @property
    def name(self):
        return f"{self.on}.{self.to.name}"

    def create_sql(self):
        privs = ", ".join([str(priv) for priv in self.privs])
        return tidy_sql(
            "GRANT",
            privs,
            "ON",
            self.on,
            self.props.render(self),
        )


class OwnershipGrant(PrivGrant):
    """
    -- Role
    GRANT OWNERSHIP
    { ON { <object_type> <object_name> | ALL <object_type_plural> IN { DATABASE <db_name> | SCHEMA <schema_name> } }
    | ON FUTURE <object_type_plural> IN { DATABASE <db_name> | SCHEMA <schema_name> }
    }
    TO ROLE <role_name>
    [ { REVOKE | COPY } CURRENT GRANTS ]

    -- Database role
    GRANT OWNERSHIP
    { ON { <object_type> <object_name> | ALL <object_type_plural> IN { DATABASE <db_name> | SCHEMA <schema_name> } }
    | ON FUTURE <object_type_plural> IN { DATABASE <db_name> | SCHEMA <schema_name> }
    }
    TO DATABASE ROLE <database_role_name>
    [ { REVOKE | COPY } CURRENT GRANTS ]
    """

    props = Props(
        to=IdentifierProp("to", eq=False, consume="role"),
    )

    privs: Annotated[list, BeforeValidator(listify)]
    on: Annotated[Resource, serialize_resource_by_name]
    to: T_Role


class AccountGrant(PrivGrant):
    """
    GRANT { globalPrivileges | ALL [ PRIVILEGES ] }
    ON ACCOUNT
    TO [ ROLE ] <role_name>
    [ WITH GRANT OPTION ]
    """

    privs: Annotated[List[GlobalPriv], BeforeValidator(listify), AfterValidator(sorted)]
    on: str = "ACCOUNT"


class AccountObjectGrant(PrivGrant):
    """
    GRANT { accountObjectPrivileges | ALL [ PRIVILEGES ] }
    ON { USER | RESOURCE MONITOR | WAREHOUSE | DATABASE | INTEGRATION | FAILOVER GROUP | REPLICATION GROUP } <object_name>
    TO [ ROLE ] <role_name>
    [ WITH GRANT OPTION ]
    """

    privs: list


class SchemaGrant(PrivGrant):
    """
    GRANT { schemaPrivileges | ALL [ PRIVILEGES ] }
    ON SCHEMA <schema_name>
    TO [ ROLE ] <role_name>
    [ WITH GRANT OPTION ]
    """

    privs: Annotated[List[SchemaPriv], BeforeValidator(listify)]
    on: Annotated[Schema, BeforeValidator(coerce_from_str(Schema))]


class SchemasGrant(PrivGrant):
    """
    GRANT { schemaPrivileges | ALL [ PRIVILEGES ] }
    ON ALL SCHEMAS IN DATABASE <db_name>
    TO [ ROLE ] <role_name>
    [ WITH GRANT OPTION ]
    """

    privs: Annotated[List[SchemaPriv], BeforeValidator(listify)]
    on: Annotated[Database, BeforeValidator(coerce_from_str(Database))]


class FutureSchemasGrant(PrivGrant):
    """
    GRANT { schemaPrivileges | ALL [ PRIVILEGES ] }
    ON FUTURE SCHEMAS IN DATABASE <db_name>
    TO [ ROLE ] <role_name>
    [ WITH GRANT OPTION ]
    """

    privs: Annotated[List[SchemaPriv], BeforeValidator(listify)]
    on: Annotated[Database, BeforeValidator(coerce_from_str(Database))]


class SchemaObjectGrant(PrivGrant):
    """
    GRANT { schemaObjectPrivileges | ALL [ PRIVILEGES ] }
    ON <object_type> <object_name>
    TO [ ROLE ] <role_name>
    [ WITH GRANT OPTION ]
    """

    privs: list


class SchemaObjectsGrant(PrivGrant):
    """
    GRANT { schemaObjectPrivileges | ALL [ PRIVILEGES ] }
    ON ALL <object_type>
    IN { DATABASE <db_name> | SCHEMA <schema_name> }
    TO [ ROLE ] <role_name>
    [ WITH GRANT OPTION ]
    """

    privs: list


class FutureSchemaObjectsGrant(PrivGrant):
    """
    GRANT { schemaObjectPrivileges | ALL [ PRIVILEGES ] }
    ON FUTURE <object_type>
    IN { DATABASE <db_name> | SCHEMA <schema_name> }
    TO [ ROLE ] <role_name>
    [ WITH GRANT OPTION ]
    """

    privs: list


class RoleGrant(Grant):
    """
    GRANT ROLE <name> TO { ROLE <parent_role_name> | USER <user_name> }
    """

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

    @property
    def fully_qualified_name(self):
        return FQN(name=self.name)

    @property
    def name(self):
        # urn:XY54321:role_grant/CI?user=SYSADMIN
        role = self.role.name
        param = "user" if self.to_user else "role"
        value = self.to_user.name if self.to_user else self.to_role.name
        return f"{role}?{param}={value}"

    def create_sql(self):
        return tidy_sql(
            "GRANT",
            self.props.render(self),
        )

    def drop_sql(self):
        return tidy_sql(
            "REVOKE ROLE",
            self.role.name,
            "FROM",
            "ROLE" if self.to_role else "USER",
            self.to_role.name or self.to_user.name,
        )
