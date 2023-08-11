# from abc import ABC
from typing import List, Union
from typing_extensions import Annotated

from pydantic import BeforeValidator

from .base import Resource, AccountScoped, Database, Schema
from .role import Role
from .validators import coerce_from_str, listify
from ..parse import _parse_grant, _parse_props
from ..props import Props, IdentifierProp, FlagProp
from ..enums import GlobalPrivs, SchemaPrivs  # SchemaObjectPrivs, AccountObjectPrivs


# Annotated[List[GlobalPrivs], coerce_from_str(Role)]


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
    props = Props(
        to=IdentifierProp("to", eq=False, consume="role"),
        with_grant_option=FlagProp("with grant option"),
    )

    privs: Annotated[List[str], BeforeValidator(listify)]
    on: str
    to: Annotated[Role, BeforeValidator(coerce_from_str(Role))]
    with_grant_option: bool = None

    # def __new__(
    #     cls, type: Union[str, FileType], **kwargs
    # ) -> Union[CSVFileFormat, JSONFileFormat, AvroFileFormat, OrcFileFormat, ParquetFileFormat, XMLFileFormat]:
    #     file_type = FileType.parse(type)
    #     file_type_cls = FileTypeMap[file_type]
    #     return file_type_cls(type=file_type, **kwargs)

    @classmethod
    def from_sql(cls, sql):
        parsed = _parse_grant(sql)
        grant_cls = Resource.classes[parsed["resource_key"]]
        props = _parse_props(grant_cls.props, parsed["remainder"])
        return grant_cls(privs=parsed["privs"], on=parsed["on"], **props)

        # return grant_cls(privs=parsed["privs"], on=parsed["on"], **props)
        # return cls(privs=parsed["privs"], on=parsed["on"], **props)


class AccountGrant(Grant):
    """
    GRANT { globalPrivileges | ALL [ PRIVILEGES ] }
    ON ACCOUNT
    TO [ ROLE ] <role_name>
    [ WITH GRANT OPTION ]
    """

    privs: Annotated[List[GlobalPrivs], BeforeValidator(listify)]
    on: str = "ACCOUNT"


class AccountObjectGrant(Grant):
    """
    GRANT { accountObjectPrivileges | ALL [ PRIVILEGES ] }
    ON { USER | RESOURCE MONITOR | WAREHOUSE | DATABASE | INTEGRATION | FAILOVER GROUP | REPLICATION GROUP } <object_name>
    TO [ ROLE ] <role_name>
    [ WITH GRANT OPTION ]
    """

    privs: list


class SchemaGrant(Grant):
    """
    GRANT { schemaPrivileges | ALL [ PRIVILEGES ] }
    ON SCHEMA <schema_name>
    TO [ ROLE ] <role_name>
    [ WITH GRANT OPTION ]
    """

    privs: Annotated[List[SchemaPrivs], BeforeValidator(listify)]
    on: Annotated[Schema, BeforeValidator(coerce_from_str(Schema))]


def test(value, **kwargs):
    print("hello world")


class SchemasGrant(Grant):
    """
    GRANT { schemaPrivileges | ALL [ PRIVILEGES ] }
    ON ALL SCHEMAS IN DATABASE <db_name>
    TO [ ROLE ] <role_name>
    [ WITH GRANT OPTION ]
    """

    privs: Annotated[List[SchemaPrivs], BeforeValidator(listify)]
    on: Annotated[Database, BeforeValidator(coerce_from_str(Database))]


class FutureSchemasGrant(Grant):
    """
    GRANT { schemaPrivileges | ALL [ PRIVILEGES ] }
    ON FUTURE SCHEMAS IN DATABASE <db_name>
    TO [ ROLE ] <role_name>
    [ WITH GRANT OPTION ]
    """

    privs: Annotated[List[SchemaPrivs], BeforeValidator(listify)]  # , BeforeValidator(test)
    on: Annotated[Database, BeforeValidator(coerce_from_str(Database))]


class SchemaObjectGrant(Grant):
    """
    GRANT { schemaObjectPrivileges | ALL [ PRIVILEGES ] }
    ON <object_type> <object_name>
    TO [ ROLE ] <role_name>
    [ WITH GRANT OPTION ]
    """

    privs: list


class SchemaObjectsGrant(Grant):
    """
    GRANT { schemaObjectPrivileges | ALL [ PRIVILEGES ] }
    ON ALL <object_type>
    IN { DATABASE <db_name> | SCHEMA <schema_name> }
    TO [ ROLE ] <role_name>
    [ WITH GRANT OPTION ]
    """

    privs: list


class FutureSchemaObjectsGrant(Grant):
    """
    GRANT { schemaObjectPrivileges | ALL [ PRIVILEGES ] }
    ON FUTURE <object_type>
    IN { DATABASE <db_name> | SCHEMA <schema_name> }
    TO [ ROLE ] <role_name>
    [ WITH GRANT OPTION ]
    """

    privs: list


# class RoleGrant(AccountLevelResource):
#     """
#     GRANT ROLE <name> TO { ROLE <parent_role_name> | USER <user_name> }
#     """

#     props = {}
#     ownable = False
#     create_statement = re.compile(
#         rf"""
#             GRANT\s+ROLE\s+
#             (?P<role>{Identifier.pattern})
#             \s+TO\s+
#             (?:
#                 ROLE\s+(?P<grantee_role>{Identifier.pattern})
#                 |
#                 USER\s+(?P<grantee_user>{Identifier.pattern})
#             )""",
#         re.VERBOSE | re.IGNORECASE,
#     )

#     def __init__(
#         self,
#         role: Union[str, Role],
#         grantee: Union[Role, User],
#     ):
#         name = ":".join((str(role), grantee.name))
#         super().__init__(name)
#         self.role = role if isinstance(role, Role) else Role.all[role]
#         if isinstance(grantee, str):
#             raise Exception
#         self.grantee = grantee
#         self.requires(self.role, self.grantee)

#     @classmethod
#     def from_sql(cls, sql: str) -> RoleGrant:
#         match = re.search(cls.create_statement, sql)

#         if match is None:
#             raise Exception
#         parsed = match.groupdict()
#         role = parsed["role"]
#         grantee = Role.all[parsed["grantee_role"]] or User.all[parsed["grantee_user"]]
#         return cls(role, grantee)

#     @property
#     def sql(self):
#         grantee_type = "ROLE" if isinstance(self.grantee, Role) else "USER"
#         return f"GRANT ROLE {self.role.name} TO {grantee_type} {self.grantee.name}"

#     @property
#     def urn(self):
#         """
#         urn:sf:us-central1.gcp:UJ63311:role_grant/
#         """

#         return URN("", "", "role_grant", "role:READERS#user:teej")


# T_Priv = TypeVar("T_Priv", GlobalPrivs, DatabasePrivs, WarehousePrivs)
# # T_Priv = Union[Type[GlobalPrivs], Type[DatabasePrivs], Type[WarehousePrivs]]


# class PrivGrant(AccountLevelResource):
#     """
#     GRANT {  { globalPrivileges         | ALL [ PRIVILEGES ] } ON ACCOUNT
#         | { accountObjectPrivileges  | ALL [ PRIVILEGES ] } ON { USER | RESOURCE MONITOR | WAREHOUSE | DATABASE | INTEGRATION | FAILOVER GROUP | REPLICATION GROUP } <object_name>
#         | { schemaPrivileges         | ALL [ PRIVILEGES ] } ON { SCHEMA <schema_name> | ALL SCHEMAS IN DATABASE <db_name> }
#         | { schemaPrivileges         | ALL [ PRIVILEGES ] } ON { FUTURE SCHEMAS IN DATABASE <db_name> }
#         | { schemaObjectPrivileges   | ALL [ PRIVILEGES ] } ON { <object_type> <object_name> | ALL <object_type_plural> IN { DATABASE <db_name> | SCHEMA <schema_name> } }
#         | { schemaObjectPrivileges   | ALL [ PRIVILEGES ] } ON FUTURE <object_type_plural> IN { DATABASE <db_name> | SCHEMA <schema_name> }
#         }
#     TO [ ROLE ] <role_name> [ WITH GRANT OPTION ]

#     globalPrivileges ::=
#         {
#             CREATE {
#                     ACCOUNT | DATA EXCHANGE LISTING | DATABASE | FAILOVER GROUP | INTEGRATION
#                     | NETWORK POLICY | REPLICATION GROUP | ROLE | SHARE | USER | WAREHOUSE
#             }
#             | APPLY { { MASKING | PASSWORD | ROW ACCESS | SESSION } POLICY | TAG }
#             | ATTACH POLICY | AUDIT |
#             | EXECUTE { ALERT | TASK }
#             | IMPORT SHARE
#             | MANAGE GRANTS
#             | MODIFY { LOG LEVEL | TRACE LEVEL | SESSION LOG LEVEL | SESSION TRACE LEVEL }
#             | MONITOR { EXECUTION | SECURITY | USAGE }
#             | OVERRIDE SHARE RESTRICTIONS | RESOLVE ALL
#         }
#         [ , ... ]

#     accountObjectPrivileges ::=
#         -- For DATABASE
#            { CREATE { DATABASE ROLE | SCHEMA } | IMPORTED PRIVILEGES | MODIFY | MONITOR | USAGE } [ , ... ]
#         -- For FAILOVER GROUP
#            { FAILOVER | MODIFY | MONITOR | REPLICATE } [ , ... ]
#         -- For INTEGRATION
#            { USAGE | USE_ANY_ROLE } [ , ... ]
#         -- For REPLICATION GROUP
#            { MODIFY | MONITOR | REPLICATE } [ , ... ]
#         -- For RESOURCE MONITOR
#            { MODIFY | MONITOR } [ , ... ]
#         -- For USER
#            { MONITOR } [ , ... ]
#         -- For WAREHOUSE
#            { MODIFY | MONITOR | USAGE | OPERATE } [ , ... ]

#     schemaPrivileges ::=
#         ADD SEARCH OPTIMIZATION
#         | CREATE {
#             ALERT | EXTERNAL TABLE | FILE FORMAT | FUNCTION
#             | MATERIALIZED VIEW | PIPE | PROCEDURE
#             | { MASKING | PASSWORD | ROW ACCESS | SESSION } POLICY
#             | SECRET | SEQUENCE | STAGE | STREAM
#             | TAG | TABLE | TASK | VIEW
#           }
#         | MODIFY | MONITOR | USAGE
#         [ , ... ]

#     schemaObjectPrivileges ::=
#         -- For ALERT
#            OPERATE [ , ... ]
#         -- For EVENT TABLE
#            { SELECT | INSERT } [ , ... ]
#         -- For FILE FORMAT, FUNCTION (UDF or external function), PROCEDURE, SECRET, or SEQUENCE
#            USAGE [ , ... ]
#         -- For PIPE
#            { MONITOR | OPERATE } [ , ... ]
#         -- For { MASKING | PASSWORD | ROW ACCESS | SESSION } POLICY or TAG
#            APPLY [ , ... ]
#         -- For external STAGE
#            USAGE [ , ... ]
#         -- For internal STAGE
#            READ [ , WRITE ] [ , ... ]
#         -- For STREAM
#            SELECT [ , ... ]
#         -- For TABLE
#            { SELECT | INSERT | UPDATE | DELETE | TRUNCATE | REFERENCES } [ , ... ]
#         -- For TASK
#            { MONITOR | OPERATE } [ , ... ]
#         -- For VIEW or MATERIALIZED VIEW
#            { SELECT | REFERENCES } [ , ... ]
#     """

#     props = {
#         "WITH_GRANT_OPTION": FlagProp("WITH GRANT OPTION"),
#     }
#     ownable = False

#     create_statement = re.compile(
#         rf"""
#             GRANT\s+
#             (?P<privs_stmt>.+)
#             (?:\s+PRIVILEGES\s+)?
#             ON\s+
#             (?P<on_stmt>.+)\s+
#             TO\s+
#             (?:ROLE\s+)?
#             (?P<grantee_role>{Identifier.pattern})
#         """,
#         re.VERBOSE | re.IGNORECASE,
#     )

#     on_statement = re.compile(
#         rf"""
#             (?P<global>
#                 ACCOUNT
#             )?
#             (?:
#                 (?P<account_object>
#                     USER |
#                     RESOURCE\s+MONITOR |
#                     WAREHOUSE |
#                     DATABASE |
#                     INTEGRATION |
#                     FAILOVER\s+GROUP |
#                     REPLICATION\s+GROUP
#                 )
#                 \s+
#                 (?P<account_object_name>{Identifier.pattern})
#             )?
#             (?P<schema_object>
#                 SCHEMA\s+
#                 (?P<schema_object_name>{Identifier.pattern})
#             )?
#             (?P<schema_object_plural>
#                 ALL\ SCHEMAS\ IN\ DATABASE
#             )?
#             (?P<future_schema_object>
#                 FUTURE\ SCHEMAS\ IN\ DATABASE
#             )?
#             (?P<class_object>
#                 CLASS
#             )?
#         """,
#         re.VERBOSE | re.IGNORECASE,
#     )

#     """
#     GRANT USAGE ON WAREHOUSE XSMALL_WH TO ROLE PUBLIC;

#     PrivGrant(
#         privs=['USAGE'],
#         on=Warehouse.all["XSMALL_WH"],
#         grantee=Role.all["PUBLIC"],
#     )
#     """

#     def __init__(
#         self,
#         privs: List[Union[str, T_Priv]],
#         on: Optional[Resource],
#         grantee: Union[None, str, Role],
#         with_grant_option: Optional[bool] = None,
#     ):
#         grantee_ = grantee if isinstance(grantee, Role) else Role.all[grantee]
#         name = ":".join([",".join([str(p) for p in privs]), on.name if on else "account", grantee_.name])
#         super().__init__(name)
#         self.privs = privs
#         self.on = on
#         self.grantee = grantee_
#         self.requires(self.grantee)
#         if self.on:
#             self.requires(self.on)

#     @classmethod
#     def from_sql(cls, sql: str) -> PrivGrant:
#         match = re.search(cls.create_statement, sql)

#         if match is None:
#             raise Exception
#         parsed = match.groupdict()
#         on_type, on_resource = cls.parse_on(parsed["on_stmt"])

#         privs = cls.parse_privs(parsed["privs_stmt"], on_type)
#         grantee = parsed["grantee_role"]
#         # grantee = parsed["grantee_role"] or parsed["grantee_user"]

#         return cls(privs, on_resource, grantee)

#     @classmethod
#     def parse_on(cls, on_stmt: str):  #  -> (Enum, str)
#         match = re.search(cls.on_statement, on_stmt)
#         if match is None:
#             raise Exception
#         parsed = match.groupdict()
#         if parsed["global"]:
#             # This is like implied account?
#             return (GlobalPrivs, None)
#         elif parsed["account_object"]:
#             account_object = parsed["account_object"].lower()
#             if account_object == "warehouse":
#                 return (WarehousePrivs, Warehouse.all[parsed["account_object_name"]])
#             elif account_object == "database":
#                 return (DatabasePrivs, Database.all[parsed["account_object_name"]])
#         elif parsed["schema_object"]:
#             return (SchemaPrivs, Schema.all[parsed["schema_object_name"]])
#         else:
#             print(parsed)
#         raise Exception(f"Not implemented for {on_stmt}")
#         # return parsed

#     @classmethod
#     def parse_privs(cls, privs_stmt: str, on_type: Type[T_Priv]) -> List[T_Priv]:
#         privs_statement = re.compile(rf"""({"|".join([e.value for e in on_type])})""")
#         privs = re.findall(privs_statement, privs_stmt)
#         # if not privs:
#         #     print(privs_stmt)
#         #     raise Exception
#         return [on_type.parse(priv) for priv in privs]

#     @property
#     def sql(self):
#         privs = ", ".join([str(p) for p in self.privs])
#         return (
#             f"GRANT {privs} ON {type(self.on).__name__.upper() if self.on else 'ACCOUNT'}"
#             + f" {self.on.name if self.on else ''} TO ROLE {self.grantee.name}"
#         )
