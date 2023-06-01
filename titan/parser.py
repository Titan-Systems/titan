import typing as t

from pyparsing import Keyword, Word, alphas, alphanums, ParseException, Optional, delimitedList


class Token:
    TABLE = Keyword("TABLE")
    FILE_FORMAT = Keyword("FILE") + Keyword("FORMAT")
    VIEW = Keyword("VIEW")


def parse_names(sql) -> t.Tuple[str, str, str]:
    # Define the grammar
    CREATE, OR, REPLACE, TEMPORARY, STAGE, PIPE, SHARE, DATABASE, SCHEMA, IF, NOT, EXISTS = map(
        Keyword, "CREATE OR REPLACE TEMPORARY STAGE PIPE SHARE DATABASE SCHEMA IF NOT EXISTS".split()
    )

    identifier = Word(alphas, alphanums + "_")
    qualified_name = delimitedList(identifier, ".", combine=True)
    resource_type = (STAGE | Token.FILE_FORMAT | PIPE | SHARE | DATABASE | SCHEMA | Token.TABLE | Token.VIEW)(
        "resource_type"
    )

    # Statement syntax
    stmt = (
        CREATE
        + Optional(OR + REPLACE)("or_replace")
        + Optional(TEMPORARY)("temporary")
        + resource_type
        + Optional(IF + NOT + EXISTS)("if_not_exists")
        + qualified_name("qualified_name")
    )

    try:
        # Return the parsed results
        res = stmt.parse_string(sql)
        name_parts = res.qualified_name.split(".")
        name = name_parts[-1]
        if res.resource_type == DATABASE:
            return (None, None, name)
        elif res.resource_type == SCHEMA:
            return (name_parts[0], None, name_parts[1])
        else:
            return [None] * (3 - len(name_parts)) + name_parts

            # return (name_parts[0], None, name_parts[1])

        # return res.name
    except ParseException as pe:
        print("Parsing failed at {0}".format(pe.loc))
        print(pe)
        return (None, None, None)
