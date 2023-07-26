import pyparsing as pp

Keyword = pp.CaselessKeyword
Identifier = pp.Word(pp.alphanums + "_", pp.alphanums + "_$") | pp.dbl_quoted_string
ScopedIdentifier = Identifier


def Keywords(keywords):
    return pp.ungroup(pp.And([Keyword(tok) for tok in keywords.split(" ")]).add_parse_action(" ".join))


CREATE = Keyword("CREATE").suppress()
OR_REPLACE = Keywords("OR REPLACE").suppress()
IF_NOT_EXISTS = Keywords("IF NOT EXISTS").suppress()
TEMPORARY = (Keyword("TEMP") | Keyword("TEMPORARY")).suppress()
TRANSIENT = Keyword("TRANSIENT").suppress()
SECURE = Keyword("SECURE").suppress()
WITH = Keyword("WITH").suppress()

REST_OF_STRING = pp.Word(pp.printables + " \n") | pp.StringEnd() | pp.Empty()


def _split_statements(sql_text):
    # Define SQL strings
    single_quote = pp.QuotedString("'", multiline=True, unquote_results=False)
    double_quote = pp.QuotedString('"', multiline=True, unquote_results=False)
    double_dollar = pp.QuotedString("$$", multiline=True, unquote_results=False, end_quote_char="$$")
    triple_dollar = pp.QuotedString("$$$", multiline=True, unquote_results=False, end_quote_char="$$$")

    # Combine all SQL strings into a single parser
    any_sql_string = pp.MatchFirst([single_quote, double_quote, double_dollar, triple_dollar])

    # Define other characters
    other_chars = pp.Word(pp.printables, excludeChars=";") | pp.White()
    semicolon = pp.Literal(";").suppress()

    # SQL Statement is any sequence of SQL strings and other characters, ended with semicolon
    sql_stmt = pp.OneOrMore(any_sql_string | other_chars).set_parse_action(" ".join) + semicolon
    result = sql_stmt.search_string(sql_text).as_list()
    return [res[0] for res in result]


def _parse_create_header(sql, expected_type):
    # resource_type = pp.Or([Keywords(type) for type in expected_types])
    header = pp.And(
        [
            CREATE,
            pp.Opt(OR_REPLACE)("or_replace"),
            pp.Opt(TEMPORARY)("temporary"),
            ...,
            Keywords(expected_type)("resource_type"),
            pp.Opt(IF_NOT_EXISTS)("if_not_exists"),
            ScopedIdentifier("resource_name"),
            REST_OF_STRING("remainder"),
        ]
    )
    try:
        results = header.parse_string(sql, parse_all=True).as_dict()
        remainder = (results["_skipped"][0] + " " + results.get("remainder", "")).strip()
        return (results["resource_name"], remainder)
    except pp.ParseException as err:
        print(err.explain())
        print("❌", "failed to parse header")
        # return None
        raise err


_default_class_resolver = CREATE + pp.Opt(OR_REPLACE) + pp.Opt(TEMPORARY) + pp.Opt(TRANSIENT) + pp.Opt(SECURE)

_resource_class_resolvers = {
    "external_function": (_default_class_resolver + Keywords("EXTERNAL FUNCTION")),
    "external_stage": (_default_class_resolver + Keyword("STAGE") + ... + Keywords("URL =")),
    "internal_stage": (_default_class_resolver + Keyword("STAGE") + ... + ~Keywords("URL =")),
    "grant": Keyword("GRANT"),
    "pipe": (_default_class_resolver + Keyword("PIPE")),
    "schema": (_default_class_resolver + Keyword("SCHEMA")),
    "shared_database": (Keywords("CREATE DATABASE") + Identifier + Keywords("FROM SHARE")),
    "database": (_default_class_resolver + Keyword("DATABASE") + ... + ~Keywords("FROM SHARE")),
    "view": (_default_class_resolver + Keyword("VIEW")),
    "warehouse": (_default_class_resolver + Keyword("WAREHOUSE")),
    "table": (_default_class_resolver + Keyword("TABLE")),
    "role": (_default_class_resolver + Keyword("ROLE")),
    "user": (_default_class_resolver + Keyword("USER")),
    "csv_file_format": (_default_class_resolver + Keywords("FILE FORMAT") + ... + Keywords("TYPE = CSV")),
    "json_file_format": (_default_class_resolver + Keywords("FILE FORMAT") + ... + Keywords("TYPE = JSON")),
    "parquet_file_format": (_default_class_resolver + Keywords("FILE FORMAT") + ... + Keywords("TYPE = PARQUET")),
    "xml_file_format": (_default_class_resolver + Keywords("FILE FORMAT") + ... + Keywords("TYPE = XML")),
    "avro_file_format": (_default_class_resolver + Keywords("FILE FORMAT") + ... + Keywords("TYPE = AVRO")),
    "orc_file_format": (_default_class_resolver + Keywords("FILE FORMAT") + ... + Keywords("TYPE = ORC")),
    "sequence": (_default_class_resolver + Keyword("SEQUENCE")),
    "task": (_default_class_resolver + Keyword("TASK")),
    "alert": (_default_class_resolver + Keyword("ALERT")),
    "tag": (_default_class_resolver + Keyword("TAG")),
    "resource_monitor": (_default_class_resolver + Keyword("RESOURCE MONITOR")),
    "dynamic_table": (_default_class_resolver + Keywords("DYNAMIC TABLE")),
    "table_stream": (_default_class_resolver + Keywords("STREAM") + ... + Keywords("ON TABLE")),
    "external_table_stream": (_default_class_resolver + Keywords("STREAM") + ... + Keywords("ON EXTERNAL TABLE")),
    "view_stream": (_default_class_resolver + Keywords("STREAM") + ... + Keywords("ON VIEW")),
    "stage_stream": (_default_class_resolver + Keywords("STREAM") + ... + Keywords("ON STAGE")),
}


def _resolve_resource_class(sql):
    for resource_key, resolver in _resource_class_resolvers.items():
        if next(pp.MatchFirst(resolver).scan_string(sql), -1) != -1:
            return resource_key
    raise Exception(f"Could not resolve resource class for SQL: {sql}")


def _consume_tokens(parser, text):
    for toks, _, end in pp.MatchFirst(parser).scan_string(text):
        if toks:
            return text[end:]
    return text
