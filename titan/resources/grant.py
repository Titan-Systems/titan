from typing import List
from typing_extensions import Annotated

from pydantic import AfterValidator, BeforeValidator

from .base import Resource, AccountScoped, Database, Schema
from .role import T_Role
from .user import T_User
from .validators import coerce_from_str, listify
from ..builder import tidy_sql
from ..parse import _parse_grant, _parse_props
from ..props import Props, IdentifierProp, FlagProp
from ..enums import GlobalPrivs, SchemaPrivs  # SchemaObjectPrivs, AccountObjectPrivs


class Grant(Resource, AccountScoped):
    resource_type = "GRANT"

    @classmethod
    def from_sql(cls, sql):
        parsed = _parse_grant(sql)
        grant_cls = Resource.classes[parsed["resource_key"]]
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
    on: str
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

    # TODO: implement instantiating grant
    # TODO: implement fallback to grant
    # def __new__(
    #     cls, type: Union[str, Grant], **kwargs
    # ) -> Union[...types...]:
    #     file_type = FileType.parse(type)
    #     file_type_cls = FileTypeMap[file_type]
    #     return file_type_cls(type=file_type, **kwargs)


class AccountGrant(PrivGrant):
    """
    GRANT { globalPrivileges | ALL [ PRIVILEGES ] }
    ON ACCOUNT
    TO [ ROLE ] <role_name>
    [ WITH GRANT OPTION ]
    """

    privs: Annotated[List[GlobalPrivs], BeforeValidator(listify), AfterValidator(sorted)]
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

    privs: Annotated[List[SchemaPrivs], BeforeValidator(listify)]
    on: Annotated[Schema, BeforeValidator(coerce_from_str(Schema))]


class SchemasGrant(PrivGrant):
    """
    GRANT { schemaPrivileges | ALL [ PRIVILEGES ] }
    ON ALL SCHEMAS IN DATABASE <db_name>
    TO [ ROLE ] <role_name>
    [ WITH GRANT OPTION ]
    """

    privs: Annotated[List[SchemaPrivs], BeforeValidator(listify)]
    on: Annotated[Database, BeforeValidator(coerce_from_str(Database))]


class FutureSchemasGrant(PrivGrant):
    """
    GRANT { schemaPrivileges | ALL [ PRIVILEGES ] }
    ON FUTURE SCHEMAS IN DATABASE <db_name>
    TO [ ROLE ] <role_name>
    [ WITH GRANT OPTION ]
    """

    privs: Annotated[List[SchemaPrivs], BeforeValidator(listify)]
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


# TODO: add a model validator to ensure to_role and to_user arent used together
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

    @property
    def name(self):
        return self.role.name

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
