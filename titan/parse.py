from typing import List, Dict, Callable, Union

import pyparsing as pp

from pyparsing import ParseException

from .enums import Scope

Keyword = pp.CaselessKeyword
Literal = pp.CaselessLiteral

Identifier = pp.Word(pp.alphanums + "_", pp.alphanums + "_$") | pp.dbl_quoted_string
# FullyQualifiedIdentifier = pp.DelimitedList(Identifier, delim=".", min=1, max=3)
FullyQualifiedIdentifier = (
    pp.DelimitedList(Identifier, delim=".", min=3, max=3)
    ^ pp.DelimitedList(Identifier, delim=".", min=2, max=2)
    ^ Identifier
)

ARROW = Literal("=>").suppress()
AS = Keyword("AS").suppress()
AT = Keyword("AT").suppress()
BEFORE = Keyword("BEFORE").suppress()
EQUALS = Literal("=").suppress()
LPAREN = Literal("(").suppress()
RPAREN = Literal(")").suppress()
TAG = Keyword("TAG").suppress()
WITH = Keyword("WITH").suppress()
ANY = pp.Word(pp.srange("[a-zA-Z0-9_]")) | pp.sgl_quoted_string
GRANT = Keyword("GRANT").suppress()
ON = Keyword("ON").suppress()
TO = Keyword("TO").suppress()


def ScopedIdentifier(scope):
    dot = Literal(".").suppress()
    if scope == Scope.ACCOUNT:
        return pp.Group(Identifier("name"))
    elif scope == Scope.DATABASE:
        return pp.Group(pp.Opt(Identifier("database") + dot) + Identifier("name"))
    elif scope == Scope.SCHEMA:
        return pp.Group(pp.Opt(pp.Opt(Identifier("database") + dot) + Identifier("schema") + dot) + Identifier("name"))
    else:
        raise Exception(f"Unsupported scope: {scope}")


def Keywords(keywords):
    words = keywords.split(" ")
    if len(words) == 1:
        return Keyword(words[0])
    # tokens = [Keyword(word) for word in words[:-1]] + [Literal(words[-1])]
    # make last token a Literal
    # return pp.original_text_for()
    return pp.ungroup(pp.And([Keyword(tok) for tok in keywords.split(" ")]).add_parse_action(" ".join))
    # return pp.original_text_for(pp.And([Keyword(word) for word in words[:-1]] + [Literal(words[-1])]))


def Literals(keywords):
    return pp.ungroup(pp.And([Literal(tok) for tok in keywords.split(" ")]).add_parse_action(" ".join))


def list_expr(expr):
    return pp.Group(pp.DelimitedList(expr) | parens(pp.DelimitedList(expr)))


def parens(expr):
    return LPAREN + expr + RPAREN


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

COLUMN = (Identifier | pp.dbl_quoted_string)("col_name") + ANY("col_type") + pp.Opt(parens(ANY()))


snowflake_sql_comment = pp.Regex(r"--.*").set_name("Snowflake SQL comment")


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
    parser = pp.OneOrMore(any_sql_string | other_chars).set_parse_action(" ".join) + semicolon
    parser = parser.ignore(pp.c_style_comment | snowflake_sql_comment)

    results = []
    for result, start, end in parser.scan_string(sql_text):
        results.append(result[0])

    # Allow last statement to not have a semicolon
    remainder = sql_text[end:]
    if remainder.strip():
        results.append(remainder)

    return results


def _parse_create_header(sql, resource_cls):
    header = pp.And(
        [
            CREATE,
            pp.Opt(OR_REPLACE)("or_replace"),
            pp.Opt(TEMPORARY)("temporary"),
            ...,
            Keywords(resource_cls.resource_type)("resource_type"),
            pp.Opt(IF_NOT_EXISTS)("if_not_exists"),
            ScopedIdentifier(resource_cls.scope)("resource_identifier"),
            REST_OF_STRING("remainder"),
        ]
    )
    try:
        results = header.parse_string(sql, parse_all=True).as_dict()
        remainder = (results["_skipped"][0] + " " + results.get("remainder", "")).strip()
        return (results["resource_identifier"], remainder)
    except pp.ParseException as err:
        print(err.explain())
        print("❌", "failed to parse header")
        # return None
        raise err


def _parse_grant(sql):
    """
    GRANT {
          { globalPrivileges         | ALL [ PRIVILEGES ] } ON ACCOUNT
        | { accountObjectPrivileges  | ALL [ PRIVILEGES ] } ON { USER | RESOURCE MONITOR | WAREHOUSE | DATABASE | INTEGRATION | FAILOVER GROUP | REPLICATION GROUP } <object_name>
        | { schemaPrivileges         | ALL [ PRIVILEGES ] } ON { SCHEMA <schema_name> | ALL SCHEMAS IN DATABASE <db_name> }
        | { schemaPrivileges         | ALL [ PRIVILEGES ] } ON { FUTURE SCHEMAS IN DATABASE <db_name> }
        | { schemaObjectPrivileges   | ALL [ PRIVILEGES ] } ON { <object_type> <object_name> | ALL <object_type_plural> IN { DATABASE <db_name> | SCHEMA <schema_name> } }
        | { schemaObjectPrivileges   | ALL [ PRIVILEGES ] } ON FUTURE <object_type_plural> IN { DATABASE <db_name> | SCHEMA <schema_name> }
    }
    """
    grant = GRANT + pp.SkipTo(ON)("privs") + ON + pp.SkipTo(TO)("on") + REST_OF_STRING("remainder")
    grant = grant.ignore(pp.c_style_comment | snowflake_sql_comment)

    try:
        results = grant.parse_string(sql, parse_all=True)
        results = results.as_dict()
        privs = [priv.strip(" ") for priv in results["privs"].split(",")]
        if "ALL" in privs or "ALL PRIVILEGES" in privs:
            if len(privs) > 1:
                raise ValueError("ALL PRIVILEGES cannot be combined with other privileges")
            privs = ["ALL"]
        grant_cls = _resolve_grant_class(results["on"])
        return {
            "privs": privs,
            "on": results["on"],
            "resource_key": grant_cls,
            "remainder": results["remainder"],
        }
    except pp.ParseException as err:
        print(err.explain())
        print("❌", "failed to parse grant")
        raise err


def _resolve_grant_class(on_stmt):
    on_stmt = on_stmt.strip(" ")

    account_object = (
        Keyword("USER")
        | Keywords("RESOURCE MONITOR")
        | Keyword("WAREHOUSE")
        | Keyword("DATABASE")
        | Keyword("INTEGRATION")
        | Keywords("FAILOVER GROUP")
        | Keywords("REPLICATION GROUP")
    )

    schema_object = (
        Keyword("ALERT")
        | Keywords("DYNAMIC TABLE")
        | Keywords("EVENT TABLE")
        | Keywords("EXTERNAL TABLE")
        | Keywords("FILE FORMAT")
        | Keyword("FUNCTION")
        | Keywords("MASKING POLICY")
        | Keywords("MATERIALIZED VIEW")
        | Keywords("PASSWORD POLICY")
        | Keyword("PIPE")
        | Keyword("PROCEDURE")
        | Keywords("ROW ACCESS POLICY")
        | Keyword("SECRET")
        | Keywords("SESSION POLICY")
        | Keyword("SEQUENCE")
        | Keyword("STAGE")
        | Keyword("STREAM")
        | Keyword("TABLE")
        | Keyword("TASK")
        | Keyword("VIEW")
    )

    schema_objects = (
        Keyword("ALERTS")
        | Keywords("DYNAMIC TABLES")
        | Keywords("EVENT TABLES")
        | Keywords("EXTERNAL TABLES")
        | Keywords("FILE FORMATS")
        | Keyword("FUNCTIONS")
        | Keywords("MASKING POLICIES")
        | Keywords("MATERIALIZED VIEWS")
        | Keywords("PASSWORD POLICIES")
        | Keyword("PIPES")
        | Keyword("PROCEDURES")
        | Keywords("ROW ACCESS POLICIES")
        | Keyword("SECRETS")
        | Keywords("SESSION POLICIES")
        | Keyword("SEQUENCES")
        | Keyword("STAGES")
        | Keyword("STREAMS")
        | Keyword("TABLES")
        | Keyword("TASKS")
        | Keyword("VIEWS")
    )

    lexicon = Lexicon(
        {
            "ACCOUNT": "account_grant",
            account_object: "account_object_grant",
            "SCHEMA": "schema_grant",
            "ALL SCHEMAS IN DATABASE": "schemas_grant",
            "FUTURE SCHEMAS IN DATABASE": "future_schemas_grant",
            schema_object: "schema_object_grant",
            Keyword("ALL") + schema_objects: "schema_objects_grant",
            Keyword("FUTURE") + schema_objects: "future_schema_objects_grant",
        }
    )

    try:
        resource_key = convert_match(lexicon, on_stmt)
        return resource_key
    except ParseException as err:
        raise ParseException(f"Could not resolve resource class for SQL: {on_stmt}") from err


def _first_match(parser, text):
    results = next(parser.scan_string(text), -1)
    if results == -1:
        return (None, None, None)
    else:
        return results


def _contains(parser, text):
    parse_results, start, end = _first_match(parser, text)
    return parse_results is not None


def _resolve_database(sql):
    if _contains(Keywords("FROM SHARE"), sql):
        return "shared_database"
    else:
        return "database"


def _resolve_file_format(sql):
    if _contains(Literals("TYPE = CSV"), sql):
        return "csv_file_format"
    elif _contains(Literals("TYPE = JSON"), sql):
        return "json_file_format"
    elif _contains(Literals("TYPE = PARQUET"), sql):
        return "parquet_file_format"
    elif _contains(Literals("TYPE = XML"), sql):
        return "xml_file_format"
    elif _contains(Literals("TYPE = AVRO"), sql):
        return "avro_file_format"
    elif _contains(Literals("TYPE = ORC"), sql):
        return "orc_file_format"


def _resolve_stage(sql):
    if _contains(Literals("URL ="), sql):
        return "external_stage"
    else:
        return "internal_stage"


def _resolve_stream(sql):
    if _contains(Literals("ON TABLE"), sql):
        return "table_stream"
    elif _contains(Literals("ON EXTERNAL TABLE"), sql):
        return "external_table_stream"
    elif _contains(Literals("ON VIEW"), sql):
        return "view_stream"
    elif _contains(Literals("ON STAGE"), sql):
        return "stage_stream"


def _resolve_storage_integration(sql):
    if _contains(Literals("STORAGE_PROVIDER = 'S3'"), sql):
        return "s3_storage_integration"
    elif _contains(Literals("STORAGE_PROVIDER = 'GCS'"), sql):
        return "gcs_storage_integration"
    elif _contains(Literals("STORAGE_PROVIDER = 'AZURE'"), sql):
        return "azure_storage_integration"


def _resolve_notification_integration(sql):
    return "email_notification_integration"
    # if _contains(Literals("TYPE = EMAIL"), sql):
    #     return "email_notification_integration"
    # elif _contains(Literals("TYPE = QUEUE"), sql):
    #     return "aws_outbound_notification_integration"


def _resolve_resource_class(sql):
    create_header = CREATE + pp.Opt(OR_REPLACE) + pp.Opt(TEMPORARY) + pp.Opt(TRANSIENT) + pp.Opt(SECURE)
    sql = _consume_tokens(create_header, sql)

    lexicon = Lexicon(
        {
            "ALERT": "alert",
            "DATABASE": _resolve_database,
            "DYNAMIC TABLE": "dynamic_table",
            "EXTERNAL FUNCTION": "external_function",
            "FILE FORMAT": _resolve_file_format,
            # "GRANT": lambda _: "grant",
            "NOTIFICATION INTEGRATION": _resolve_notification_integration,
            "PIPE": "pipe",
            "RESOURCE MONITOR": "resource_monitor",
            "ROLE": "role",
            "SCHEMA": "schema",
            "SEQUENCE": "sequence",
            "STAGE": _resolve_stage,
            "STORAGE INTEGRATION": _resolve_storage_integration,
            "STREAM": _resolve_stream,
            "TABLE": "table",
            "TAG": "tag",
            "TASK": "task",
            "USER": "user",
            "VIEW": "view",
            "WAREHOUSE": "warehouse",
        }
    )

    try:
        resource_key = convert_match(lexicon, sql)
        return resource_key
    except ParseException as err:
        raise ParseException(f"Could not resolve resource class for SQL: {sql}") from err


class Lexicon:
    def __init__(self, lexicon: Dict[Union[str, pp.ParserElement], Union[str, Callable[[str], str]]]):
        self._words = []
        self._actions = []
        idx = 0
        for word, action in lexicon.items():
            if isinstance(word, str):
                word = Keywords(word)
            word = word.set_results_name(str(idx))
            self._words.append(word)
            self._actions.append(action)
            idx += 1

    @property
    def parser(self):
        return pp.MatchFirst(self._words)

    def get_action(self, parse_result):
        result_names = list(parse_result.as_dict().keys())
        idx = int(result_names[0])
        return self._actions[idx]


def convert_match(lexicon: Lexicon, text):
    parser = pp.StringStart() + lexicon.parser
    parse_result, _, end = _first_match(parser, text)
    if parse_result is None:
        raise ParseException(f"Could not match {text}")
    action_or_str = lexicon.get_action(parse_result)
    if callable(action_or_str):
        action = action_or_str
        return action(text[end:])
    else:
        return action_or_str


def _consume_tokens(parser, text):
    for _, _, end in parser.scan_string(text):
        return text[end:]
    return text


def _format_parser(parser):
    if hasattr(parser, "exprs"):
        return " ".join([_format_parser(expr) for expr in parser.exprs])
    elif hasattr(parser, "expr"):
        return _format_parser(parser.expr)
    else:
        return str(parser)


def _first_expr(parser):
    if hasattr(parser, "exprs"):
        return _first_expr(parser.exprs[0])
    elif hasattr(parser, "expr"):
        return _first_expr(parser.expr)
    else:
        return parser


def _best_guess_failing_parser(parser, text):
    first_token = text.split(" ")[0]
    for expr in parser.exprs:
        print(">>>>>", first_token, _format_parser(expr))
        if first_token in _format_parser(expr):
            return expr


def _parser_has_results_name(parser, name):
    if parser.resultsName == name:
        return True
    if hasattr(parser, "exprs"):
        return any([_parser_has_results_name(expr, name) for expr in parser.exprs])
    elif hasattr(parser, "expr"):
        return False


def _marker(name):
    return pp.Empty().set_parse_action(lambda s, loc, toks, marker=name: marker)


def _parse_props(props, sql):
    if sql.strip() == "":
        return {}

    found_props = {}

    lexicon = []
    for prop_kwarg, prop in props.props.items():
        lexicon.append(prop.parser.copy() + _marker(prop_kwarg))

    parser = pp.MatchFirst(lexicon).ignore(pp.c_style_comment)
    if props.start_token:
        sql = _consume_tokens(props.start_token, sql)

    remainder = sql
    prev_end = 0

    for parse_results, start, end in parser.scan_string(sql):
        # Check if we skipped any text
        if len(sql[prev_end:start].strip()) > 0:
            # raise Exception(f"Failed to parse prop {sql[prev_end:start]}")
            raise ParseException(f"Failed to parse prop {sql[prev_end:start]}")

        prop_kwarg = parse_results[-1]
        prop = props[prop_kwarg]

        if "prop_value" not in parse_results:
            raise RuntimeError(f"Parsed prop {prop} did not return a prop_value")

        prop_value = parse_results["prop_value"]

        if isinstance(prop_value, pp.ParseResults):
            prop_value = prop_value.as_list()

        found_props[prop_kwarg] = prop.typecheck(prop_value)
        remainder = sql[end:].strip(" ")
        prev_end = end

        print("ok")
        if remainder == "":
            break

    if len(remainder) > 0:
        formatted_sql = "\n".join(["  " + line.strip() for line in remainder.splitlines()])
        # failing_parser = _best_guess_failing_parser(parser, remainder)
        formatted_props = _format_props(props)
        raise Exception(f"Failed to parse props.\nSQL: \n```\n{formatted_sql}\n```\nProps:\n{formatted_props}\n\n")
    return found_props


def _format_props(props):
    buf = []
    for prop_kwarg, prop in props.props.items():
        buf.append(f"{type(prop).__name__}('{prop_kwarg}') -> {str(prop.parser)}")
    return "\n".join(buf)
