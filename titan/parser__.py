# import re

# from typing import Tuple

# from pyparsing import Keyword, Word, alphas, alphanums, ParseException, Optional, delimitedList  # type: ignore


# def parse(sql):
#     unsupported = set(["drop", "alter"])
#     tokens = sql.split(" ")
#     if tokens[0]:
#         if tokens[0].lower() == "create":
#             return parse_create(sql)
#         elif tokens[0].lower() == "use":
#             return parse_use(sql)
#         elif tokens[0].lower() == "grant":
#             return parse_grant(sql)
#         elif tokens[0].lower() == "copy":
#             return parse_copy(sql)
#         elif tokens[0].lower() in unsupported:
#             return
#         else:
#             raise Exception(f"Unsupported SQL command: {tokens[0]}")


# def parse_grant(sql):
#     """
#     GRANT {  { globalPrivileges         | ALL [ PRIVILEGES ] } ON ACCOUNT
#         | { accountObjectPrivileges  | ALL [ PRIVILEGES ] } ON { USER | RESOURCE MONITOR | WAREHOUSE | DATABASE | INTEGRATION | FAILOVER GROUP | REPLICATION GROUP } <object_name>
#         | { schemaPrivileges         | ALL [ PRIVILEGES ] } ON { SCHEMA <schema_name> | ALL SCHEMAS IN DATABASE <db_name> }
#         | { schemaPrivileges         | ALL [ PRIVILEGES ] } ON { FUTURE SCHEMAS IN DATABASE <db_name> }
#         | { schemaObjectPrivileges   | ALL [ PRIVILEGES ] } ON { <object_type> <object_name> | ALL <object_type_plural> IN { DATABASE <db_name> | SCHEMA <schema_name> } }
#         | { schemaObjectPrivileges   | ALL [ PRIVILEGES ] } ON FUTURE <object_type_plural> IN { DATABASE <db_name> | SCHEMA <schema_name> }
#         }
#     TO [ ROLE ] <role_name> [ WITH GRANT OPTION ]
#     """
#     pattern = re.compile(r"GRANT\s+(?P<privileges>.+)\s+ON\s+(?P<resource>.+)\s+TO\s+(?P<role>.+)")
#     match = pattern.match(sql)
#     if match:
#         return match.groupdict()


# def parse_create(sql):
#     pass


# def parse_use(sql):
#     pass


# def parse_copy(sql):
#     pass


# # def generate_prop_tokens(sql):
# #     tokens = sql.split(" ")

# #     token_identifier = re.compile(r"[A-Z][A-Z0-9_]*")

# #     def peek():
# #         return tokens[0]

# #     # ignore = set("[],")
# #     equals = "="
# #     props = []
# #     prop = []

# #     while tokens:
# #         token, tokens = tokens[0], tokens[1:]
# #         if token_identifier.match(token):
# #             prop.append(token)
# #         else:
# #             if prop:
# #                 props.append(" ".join(prop))
# #             prop = []

# #     if prop:
# #         props.append(" ".join(prop))
# #     return props


# # def parse_table_props(sql):
# #     """
# #     Syntax:
# #     [ CLUSTER BY ( <expr> [ , <expr> , ... ] ) ]
# #     [ STAGE_FILE_FORMAT = ( { FORMAT_NAME = '<file_format_name>'
# #                             | TYPE = { CSV | JSON | AVRO | ORC | PARQUET | XML } [ formatTypeOptions ] } ) ]
# #     [ STAGE_COPY_OPTIONS = ( copyOptions ) ]
# #     [ DATA_RETENTION_TIME_IN_DAYS = <integer> ]
# #     [ MAX_DATA_EXTENSION_TIME_IN_DAYS = <integer> ]
# #     [ CHANGE_TRACKING = { TRUE | FALSE } ]
# #     [ DEFAULT_DDL_COLLATION = '<collation_specification>' ]
# #     [ COPY GRANTS ]
# #     [ [ WITH ] ROW ACCESS POLICY <policy_name> ON ( <col_name> [ , <col_name> ... ] ) ]
# #     [ [ WITH ] TAG ( <tag_name> = '<tag_value>' [ , <tag_name> = '<tag_value>' , ... ] ) ]
# #     [ COMMENT = '<string_literal>' ]
# # """


# class Token:
#     TABLE = Keyword("TABLE")
#     FILE_FORMAT = Keyword("FILE") + Keyword("FORMAT")
#     VIEW = Keyword("VIEW")


# def parse_names(sql) -> Tuple[str, str, str]:
#     # Define the grammar
#     CREATE, OR, REPLACE, TEMPORARY, STAGE, PIPE, SHARE, DATABASE, SCHEMA, IF, NOT, EXISTS = map(
#         Keyword, "CREATE OR REPLACE TEMPORARY STAGE PIPE SHARE DATABASE SCHEMA IF NOT EXISTS".split()
#     )

#     identifier = Word(alphas, alphanums + "_")
#     qualified_name = delimitedList(identifier, ".", combine=True)
#     resource_type = (STAGE | Token.FILE_FORMAT | PIPE | SHARE | DATABASE | SCHEMA | Token.TABLE | Token.VIEW)(
#         "resource_type"
#     )

#     # Statement syntax
#     stmt = (
#         CREATE
#         + Optional(OR + REPLACE)("or_replace")
#         + Optional(TEMPORARY)("temporary")
#         + resource_type
#         + Optional(IF + NOT + EXISTS)("if_not_exists")
#         + qualified_name("qualified_name")
#     )

#     try:
#         # Return the parsed results
#         res = stmt.parse_string(sql)
#         name_parts = res.qualified_name.split(".")
#         name = name_parts[-1]
#         if res.resource_type == DATABASE:
#             return (None, None, name)
#         elif res.resource_type == SCHEMA:
#             return (name_parts[0], None, name_parts[1])
#         else:
#             return [None] * (3 - len(name_parts)) + name_parts

#             # return (name_parts[0], None, name_parts[1])

#         # return res.name
#     except ParseException as pe:
#         print("Parsing failed at {0}".format(pe.loc))
#         print(pe)
#         return (None, None, None)
