import pyparsing as pp

Keyword = pp.CaselessKeyword
Literal = pp.CaselessLiteral
Identifier = pp.Word(pp.alphanums + "_", pp.alphanums + "_$") | pp.dbl_quoted_string
ScopedIdentifier = Identifier


def Keywords(keywords):
    return pp.ungroup(pp.And([Keyword(tok) for tok in keywords.split(" ")]).add_parse_action(" ".join))


def Literals(keywords):
    return pp.ungroup(pp.And([Literal(tok) for tok in keywords.split(" ")]).add_parse_action(" ".join))


CREATE = Keyword("CREATE").suppress()
OR_REPLACE = Keywords("OR REPLACE").suppress()
IF_NOT_EXISTS = Keywords("IF NOT EXISTS").suppress()
TEMPORARY = (Keyword("TEMP") | Keyword("TEMPORARY")).suppress()
TRANSIENT = Keyword("TRANSIENT").suppress()
SECURE = Keyword("SECURE").suppress()
WITH = Keyword("WITH").suppress()

REST_OF_STRING = pp.Word(pp.printables + " \n") | pp.StringEnd() | pp.Empty()


STORAGE_INTEGRATION = Keywords("STORAGE INTEGRATION")
NOTIFICATION_INTEGRATION = Keywords("NOTIFICATION INTEGRATION")


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


def _scan(parser, text):
    results = next(parser.scan_string(text), -1)
    return None if results == -1 else results


def _matches(parser, text):
    return _scan(parser, text) is not None


def _resolve_database(sql):
    if _matches(Keywords("FROM SHARE"), sql):
        return "shared_database"
    else:
        return "database"


def _resolve_file_format(sql):
    if _matches(Literals("TYPE = CSV"), sql):
        return "csv_file_format"
    elif _matches(Literals("TYPE = JSON"), sql):
        return "json_file_format"
    elif _matches(Literals("TYPE = PARQUET"), sql):
        return "parquet_file_format"
    elif _matches(Literals("TYPE = XML"), sql):
        return "xml_file_format"
    elif _matches(Literals("TYPE = AVRO"), sql):
        return "avro_file_format"
    elif _matches(Literals("TYPE = ORC"), sql):
        return "orc_file_format"


def _resolve_stage(sql):
    if _matches(Literals("URL ="), sql):
        return "external_stage"
    else:
        return "internal_stage"


def _resolve_stream(sql):
    if _matches(Literals("ON TABLE"), sql):
        return "table_stream"
    elif _matches(Literals("ON EXTERNAL TABLE"), sql):
        return "external_table_stream"
    elif _matches(Literals("ON VIEW"), sql):
        return "view_stream"
    elif _matches(Literals("ON STAGE"), sql):
        return "stage_stream"


def _resolve_storage_integration(sql):
    if _matches(Literals("STORAGE_PROVIDER = 'S3'"), sql):
        return "s3_storage_integration"
    elif _matches(Literals("STORAGE_PROVIDER = 'GCS'"), sql):
        return "gcs_storage_integration"
    elif _matches(Literals("STORAGE_PROVIDER = 'AZURE'"), sql):
        return "azure_storage_integration"


def _resolve_notification_integration(sql):
    return "email_notification_integration"
    # if _matches(Literals("TYPE = EMAIL"), sql):
    #     return "email_notification_integration"
    # elif _matches(Literals("TYPE = QUEUE"), sql):
    #     return "aws_outbound_notification_integration"


def _resolve_resource_class(sql):
    create_header = CREATE + pp.Opt(OR_REPLACE) + pp.Opt(TEMPORARY) + pp.Opt(TRANSIENT) + pp.Opt(SECURE)
    sql = _consume_tokens(create_header, sql)

    resource_key = scan(
        {
            "ALERT": lambda _: "alert",
            "DATABASE": _resolve_database,
            "DYNAMIC TABLE": lambda _: "dynamic_table",
            "EXTERNAL FUNCTION": lambda _: "external_function",
            "FILE FORMAT": _resolve_file_format,
            "GRANT": lambda _: "grant",
            "NOTIFICATION INTEGRATION": _resolve_notification_integration,
            "PIPE": lambda _: "pipe",
            "RESOURCE MONITOR": lambda _: "resource_monitor",
            "ROLE": lambda _: "role",
            "SCHEMA": lambda _: "schema",
            "SEQUENCE": lambda _: "sequence",
            "STAGE": _resolve_stage,
            "STORAGE INTEGRATION": _resolve_storage_integration,
            "STREAM": _resolve_stream,
            "TABLE": lambda _: "table",
            "TAG": lambda _: "tag",
            "TASK": lambda _: "task",
            "USER": lambda _: "user",
            "VIEW": lambda _: "view",
            "WAREHOUSE": lambda _: "warehouse",
        },
        sql,
    )
    if resource_key is None:
        raise Exception(f"Could not resolve resource class for SQL: {sql}")

    return resource_key


def scan(lexicon, text):
    parser = pp.MatchFirst([Keywords(tok) for tok in lexicon.keys()])
    for tokens, _, end in parser.scan_string(text):
        action = lexicon[tokens[0]]
        return action(text[end:])


def _consume_tokens(parser, text):
    for tokens, _, end in pp.MatchFirst(parser).scan_string(text):
        if tokens:
            return text[end:]
    return text
