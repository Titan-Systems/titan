# from __future__ import annotations

# import re

# from typing import Optional, Union, List, TypeVar, Type

# from .parseable_enum import ParseableEnum
# from .props import Identifier, FlagProp
# from .resource import AccountLevelResource, Resource
# from .database import Database

# # from .role import Role
# from .schema import Schema
# from .user import User
# from .warehouse import Warehouse

# from .urn import URN


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


# class GlobalPrivs(ParseableEnum):
#     CREATE_ACCOUNT = "CREATE ACCOUNT"
#     CREATE_DATA_EXCHANGE_LISTING = "CREATE DATA EXCHANGE LISTING"
#     CREATE_DATABASE = "CREATE DATABASE"
#     CREATE_FAILOVER_GROUP = "CREATE FAILOVER GROUP"
#     CREATE_INTEGRATION = "CREATE INTEGRATION"
#     CREATE_NETWORK_POLICY = "CREATE NETWORK POLICY"
#     CREATE_REPLICATION_GROUP = "CREATE REPLICATION GROUP"
#     CREATE_ROLE = "CREATE ROLE"
#     CREATE_SHARE = "CREATE SHARE"
#     CREATE_USER = "CREATE USER"
#     CREATE_WAREHOUSE = "CREATE WAREHOUSE"
#     APPLY_MASKING_POLICY = "APPLY MASKING POLICY"
#     APPLY_PASSWORD_POLICY = "APPLY PASSWORD POLICY"
#     APPLY_ROW_ACCESS_POLICY = "APPLY ROW ACCESS POLICY"
#     APPLY_SESSION_POLICY = "APPLY SESSION POLICY"
#     APPLY_TAG = "APPLY TAG"
#     ATTACH_POLICY = "ATTACH POLICY"
#     AUDIT = "AUDIT"
#     EXECUTE_ALERT = "EXECUTE ALERT"
#     EXECUTE_TASK = "EXECUTE TASK"
#     IMPORT_SHARE = "IMPORT SHARE"
#     MANAGE_GRANTS = "MANAGE GRANTS"
#     MODIFY_LOG_LEVEL = "MODIFY LOG LEVEL"
#     MODIFY_TRACE_LEVEL = "MODIFY TRACE LEVEL"
#     MODIFY_SESSION_LOG_LEVEL = "MODIFY SESSION LOG LEVEL"
#     MODIFY_SESSION_TRACE_LEVEL = "MODIFY SESSION TRACE LEVEL"
#     MONITOR_EXECUTION = "MONITOR EXECUTION"
#     MONITOR_SECURITY = "MONITOR SECURITY"
#     MONITOR_USAGE = "MONITOR USAGE"
#     OVERRIDE_SHARE_RESTRICTIONS = "OVERRIDE SHARE RESTRICTIONS"
#     RESOLVE_ALL = "RESOLVE ALL"


# class DatabasePrivs(ParseableEnum):
#     CREATE_DATABASE_ROLE = "CREATE DATABASE ROLE"
#     CREATE_SCHEMA = "CREATE SCHEMA"
#     IMPORTED_PRIVILEGES = "IMPORTED PRIVILEGES"
#     MODIFY = "MODIFY"
#     MONITOR = "MONITOR"
#     USAGE = "USAGE"


# class SchemaPrivs(ParseableEnum):
#     ADD_SEARCH_OPTIMIZATION = "ADD SEARCH OPTIMIZATION"
#     CREATE_ALERT = "CREATE ALERT"
#     CREATE_EXTERNAL_TABLE = "CREATE EXTERNAL TABLE"
#     CREATE_FILE_FORMAT = "CREATE FILE FORMAT"
#     CREATE_FUNCTION = "CREATE FUNCTION"
#     CREATE_MATERIALIZED_VIEW = "CREATE MATERIALIZED VIEW"
#     CREATE_PIPE = "CREATE PIPE"
#     CREATE_PROCEDURE = "CREATE PROCEDURE"
#     CREATE_MASKING_POLICY = "CREATE MASKING POLICY"
#     CREATE_PASSWORD_POLICY = "CREATE PASSWORD POLICY"
#     CREATE_ROW_ACCESS_POLICY = "CREATE ROW ACCESS POLICY"
#     CREATE_SESSION_POLICY = "CREATE SESSION POLICY"
#     CREATE_SECRET = "CREATE SECRET"
#     CREATE_SEQUENCE = "CREATE SEQUENCE"
#     CREATE_STAGE = "CREATE STAGE"
#     CREATE_STREAM = "CREATE STREAM"
#     CREATE_TAG = "CREATE TAG"
#     CREATE_TABLE = "CREATE TABLE"
#     CREATE_TASK = "CREATE TASK"
#     CREATE_VIEW = "CREATE VIEW"
#     MODIFY = "MODIFY"
#     MONITOR = "MONITOR"
#     USAGE = "USAGE"


# class WarehousePrivs(ParseableEnum):
#     MODIFY = "MODIFY"
#     MONITOR = "MONITOR"
#     USAGE = "USAGE"
#     OPERATE = "OPERATE"


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
