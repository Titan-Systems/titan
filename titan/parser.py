from pyparsing import Keyword, Word, alphas, alphanums, ParseException, Optional


class Token:
    TABLE = Keyword("TABLE")
    FILE_FORMAT = Keyword("FILE") + Keyword("FORMAT")
    VIEW = Keyword("VIEW")


def parse_name(sql):
    # Define the grammar
    CREATE, OR, REPLACE, TEMPORARY, STAGE, PIPE, SHARE, DATABASE, SCHEMA, IF, NOT, EXISTS = map(
        Keyword, "CREATE OR REPLACE TEMPORARY STAGE PIPE SHARE DATABASE SCHEMA IF NOT EXISTS".split()
    )

    identifier = Word(alphas, alphanums + "_")
    entity_type = (STAGE | Token.FILE_FORMAT | PIPE | SHARE | DATABASE | SCHEMA | Token.TABLE | Token.VIEW)(
        "entity_type"
    )

    # Statement syntax
    stmt = (
        CREATE
        + Optional(OR + REPLACE)("or_replace")
        + Optional(TEMPORARY)("temporary")
        + entity_type
        + Optional(IF + NOT + EXISTS)("if_not_exists")
        + identifier("name")
    )

    try:
        # Return the parsed results
        res = stmt.parse_string(sql)
        return res.name
    except ParseException as pe:
        print("Parsing failed at {0}".format(pe.loc))
        print(pe)
        return None
