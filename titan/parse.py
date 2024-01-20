import re

from typing import List, Dict, Callable, Union

import pyparsing as pp

from pyparsing import ParseException

from .builder import SQL
from .enums import Scope
from .identifiers import FQN, URN
from .scope import DatabaseScope, SchemaScope

Keyword = pp.CaselessKeyword
Literal = pp.CaselessLiteral

Identifier = pp.Word(pp.alphanums + "_", pp.alphanums + "_$") | pp.dbl_quoted_string
FullyQualifiedIdentifier = (
    pp.delimited_list(Identifier, delim=".", min=3, max=3)
    ^ pp.delimited_list(Identifier, delim=".", min=2, max=2)
    ^ Identifier
)

StringLiteral = pp.QuotedString("'", multiline=False, unquote_results=True) | pp.QuotedString(
    "$$", multiline=True, unquote_results=True
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
ANY = pp.Word(pp.srange("[a-zA-Z0-9_]")) | StringLiteral
GRANT = Keyword("GRANT").suppress()
ON = Keyword("ON").suppress()
TO = Keyword("TO").suppress()


def Keywords(keywords):
    words = keywords.split(" ")
    if len(words) == 1:
        return Keyword(words[0])
    return pp.ungroup(pp.And([Keyword(tok) for tok in keywords.split(" ")]).add_parse_action(" ".join))


def Literals(keywords):
    return pp.ungroup(pp.And([Literal(tok) for tok in keywords.split(" ")]).add_parse_action(" ".join))


def _in_parens(expr):
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


def _make_scoped_identifier(identifier_list, scope):
    if len(identifier_list) == 1:
        return {"name": identifier_list[0]}
    elif len(identifier_list) == 2:
        if scope == Scope.DATABASE or isinstance(scope, DatabaseScope):
            return {"database": identifier_list[0], "name": identifier_list[1]}
        elif scope == Scope.SCHEMA or isinstance(scope, SchemaScope):
            return {"schema": identifier_list[0], "name": identifier_list[1]}
    elif len(identifier_list) == 3:
        return {"database": identifier_list[0], "schema": identifier_list[1], "name": identifier_list[2]}
    else:
        raise Exception(f"Unsupported identifier list: {identifier_list}")


def _parse_create_header(sql, resource_cls):
    header = pp.And(
        [
            CREATE,
            pp.Opt(OR_REPLACE)("or_replace"),
            pp.Opt(TEMPORARY)("temporary"),
            ...,
            Keywords(str(resource_cls.resource_type))("resource_type"),
            pp.Opt(IF_NOT_EXISTS)("if_not_exists"),
            FullyQualifiedIdentifier("resource_identifier"),
            REST_OF_STRING("remainder"),
        ]
    )
    try:
        results = header.parse_string(sql, parse_all=True).as_dict()
        remainder = (results["_skipped"][0] + " " + results.get("remainder", "")).strip(" ;")
        identifier = _make_scoped_identifier(results["resource_identifier"], resource_cls.scope)
        return (identifier, remainder)
    except pp.ParseException as err:
        raise pp.ParseException("Failed to parse header") from err


def _parse_grant(sql: Union[str, SQL]):
    """
    GRANT {
          { globalPrivileges         | ALL [ PRIVILEGES ] } ON ACCOUNT
        | { accountObjectPrivileges  | ALL [ PRIVILEGES ] } ON { USER | RESOURCE MONITOR | WAREHOUSE | DATABASE | INTEGRATION | FAILOVER GROUP | REPLICATION GROUP } <object_name>
        | { schemaPrivileges         | ALL [ PRIVILEGES ] } ON { SCHEMA <schema_name> | ALL SCHEMAS IN DATABASE <db_name> }
        | { schemaPrivileges         | ALL [ PRIVILEGES ] } ON { FUTURE SCHEMAS IN DATABASE <db_name> }
        | { schemaObjectPrivileges   | ALL [ PRIVILEGES ] } ON { <object_type> <object_name> | ALL <object_type_plural> IN { DATABASE <db_name> | SCHEMA <schema_name> } }
        | { schemaObjectPrivileges   | ALL [ PRIVILEGES ] } ON FUTURE <object_type_plural> IN { DATABASE <db_name> | SCHEMA <schema_name> }
    }
    TO [ ROLE ] <role_name> [ WITH GRANT OPTION ]
    """

    if isinstance(sql, SQL):
        sql = str(sql)

    # Check for role grant
    if _contains(Keywords("GRANT ROLE"), sql):
        raise NotImplementedError("Role grant not supported")
        # return {"resource_key": "role_grant"}

    # Check for ownership grant
    if _contains(Keywords("GRANT OWNERSHIP"), sql):
        raise NotImplementedError("Ownership grant not supported")
        # return {"resource_key": "ownership_grant"}

    grant = (
        GRANT
        + pp.SkipTo(ON)("privs")
        + ON
        + pp.SkipTo(TO)("on_stmt")
        + TO
        + pp.Opt(Keyword("ROLE").suppress())
        + Identifier("to")
        + pp.Opt(Keywords("WITH GRANT OPTION").suppress())
    )
    grant = grant.ignore(pp.c_style_comment | snowflake_sql_comment)

    try:
        results = grant.parse_string(sql, parse_all=True)
        results = results.as_dict()

        privs = [priv.strip(" ") for priv in results["privs"].split(",")]
        if len(privs) > 1:
            raise NotImplementedError("Multi-priv grants are not supported")

        on_stmt = results.pop("on_stmt").strip()
        if on_stmt == "ACCOUNT":
            on_keyword = "on"
            on_arg = on_stmt
        else:
            on_keyword = "on_" + "_".join(on_stmt.split(" ")[:-1]).lower()
            on_arg = on_stmt.split(" ")[-1]

        return {
            "priv": privs[0],
            on_keyword: on_arg,
            "to": results["to"],
        }
    except pp.ParseException as err:
        raise pp.ParseException("Failed to parse grant") from err


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
        # Check if we skipped any text. Since `parser` is a MatchFirst, skipped text is a sign
        # that our SQL is invalid or our parser is incomplete.
        if len(sql[prev_end:start].strip()) > 0:
            raise ParseException(f"Failed to parse prop {sql[prev_end:start]}")

        prop_kwarg = parse_results[-1]
        prop = props[prop_kwarg]

        if "prop_value" not in parse_results:
            raise RuntimeError(f"Parsed prop {prop} did not return a prop_value")

        prop_value = parse_results["prop_value"]

        if isinstance(prop_value, pp.ParseResults):
            prop_value = prop_value.as_list()

        try:
            found_props[prop_kwarg] = prop.typecheck(prop_value)
        except ValueError as err:
            raise ValueError(f"Parsed prop {prop_kwarg} with value {prop_value} failed typechecking") from err
        except ParseException:
            raise ValueError(f"Parsed prop {prop_kwarg}={prop} with value {prop_value} failed typechecking")
        remainder = sql[end:].strip(" ")
        prev_end = end
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


def _parse_column(sql):
    collate = Keyword("COLLATE").suppress() + ANY("collate")
    comment = Keyword("COMMENT").suppress() + ANY("comment")
    not_null = Keywords("NOT NULL").set_parse_action(lambda _: True)("not_null")
    constraint = Keyword("UNIQUE") ^ Keywords("PRIMARY KEY") ^ (Keyword("CONSTRAINT").suppress() + ANY())
    # TODO: rest of column properties
    constraint = constraint.set_parse_action(lambda toks: toks[0])("constraint")
    column = (
        Identifier("name")
        + pp.ungroup((ANY() + _in_parens(ANY())) ^ ANY())("data_type")
        + pp.Opt(collate)
        + pp.Opt(comment)
        + pp.Opt(not_null)
        + pp.Opt(constraint)
        + REST_OF_STRING("remainder")
    )
    try:
        results = column.parse_string(sql, parse_all=True)
        return results.as_dict()
    except pp.ParseException as err:
        raise pp.ParseException("Failed to parse column") from err


def _parse_table_schema(sql):
    columns_blob, start, end = _first_match(pp.original_text_for(pp.nested_expr()), sql)
    columns_blob = columns_blob[0].strip("()")
    columns = []
    while columns_blob:
        col = _parse_column(columns_blob)
        columns_blob = col.pop("remainder", "")
        columns.append(col)
        if columns_blob and columns_blob[0] == ",":
            columns_blob = columns_blob[1:]

    # TODO: outofline constraints

    remainder = sql[0:start] + " " + sql[end:]
    table_schema = {"columns": columns}
    return (table_schema, remainder)


def _parse_stage_path(stage_path_str):
    stage_path = {}
    if not stage_path_str.startswith("@"):
        raise Exception(f"Import location must be a stage: {stage_path_str} does not start with @")
    if "/" in stage_path_str:
        stage_path["stage_name"] = stage_path_str[1:].split("/")[0]
        stage_path["path"] = "/".join(stage_path_str[1:].split("/")[1:])
    else:
        stage_path["stage_name"] = stage_path_str[1:]
        stage_path["path"] = ""
    return stage_path


def _parse_dynamic_table_text(text: str):
    """
    To the annoyance of some, the only way to get the canonical values of refresh_mode, initialize, and as
    is to parse it out of the `text` field of the dynamic table. This function does that.

    In a SHOW DYNAMIC TABLES statement, the `text` field is the DDL statement for the dynamic table. Snowflake
    (strangely) decided to use the mostly unaltered SQL statement as originally run, preserving some whitespace.

    For example:
        CREATE OR REPLACE DYNAMIC TABLE
            product (id INT)
            COMMENT = 'this is a comment'
            lag = '20 minutes'
            refresh_mode = 'AUTO'
            initialize = 'ON_CREATE'
            warehouse = CI
            AS
                SELECT id FROM upstream;

    """

    # Remove newlines
    text = text.replace("\n", " ")

    # Parse refresh_mode
    match_refresh_mode = re.search(r"refresh_mode\s*=\s*'(AUTO|FULL|INCREMENTAL)'", text)
    refresh_mode = match_refresh_mode.group(1) if match_refresh_mode else None

    # Parse initialize
    match_initialize = re.search(r"initialize\s*=\s*'(ON_CREATE|ON_SCHEDULE)'", text)
    initialize = match_initialize.group(1) if match_initialize else None

    # Parse as
    match_as = re.search(r"\s+AS\s+(.*)$", text)
    as_ = match_as.group(1) if match_as else None

    return (
        refresh_mode,
        initialize,
        as_,
    )


def parse_function_name(header: str):
    """
    Example:
        "CREATE_OR_UPDATE_SCHEMA(CONFIG OBJECT, YAML VARCHAR, DRY_RUN BOOLEAN):OBJECT"
        "THIS()SUCKS():OBJECT"
        "A(BCDEFG:OBJEC)T(ARG1 OBJECT, FOO BAR.BAZ():VARIANT VARCHAR):OBJECT"
    """
    header = header.strip('"')
    # Only handling the simple case because adverse cases are likely impossible to parse
    prefix, _, _ = header.partition("(")
    return prefix


def parse_identifier(identifier: str, is_schema=False) -> FQN:
    # TODO: This needs to support periods and question marks in double quoted identifiers
    scoped_name, param_str = identifier.split("?") if "?" in identifier else (identifier, "")
    params = {}
    if param_str:
        for param in param_str.split("&"):
            k, v = param.split("=")
            params[k] = v

    arg_types = []
    if "(" in scoped_name:
        args_start = scoped_name.find("(")
        scoped_name, args_str = scoped_name[:args_start], scoped_name[args_start:]
        arg_types = [arg.strip() for arg in args_str.strip("()").split(",")]

    name_parts = list(FullyQualifiedIdentifier.parse_string(scoped_name, parse_all=True))
    if len(name_parts) == 1:
        return FQN(
            name=name_parts[0],
            params=params,
            arg_types=arg_types,
        )
    elif len(name_parts) == 2:
        if is_schema:
            return FQN(
                database=name_parts[0],
                name=name_parts[1],
                params=params,
                arg_types=arg_types,
            )
        else:
            return FQN(
                schema=name_parts[0],
                name=name_parts[1],
                params=params,
                arg_types=arg_types,
            )
    elif len(name_parts) == 3:
        return FQN(
            database=name_parts[0],
            schema=name_parts[1],
            name=name_parts[2],
            params=params,
            arg_types=arg_types,
        )
    raise Exception(f"Failed to parse identifier: {identifier}")


def parse_URN(urn_str: str) -> URN:
    parts = urn_str.split(":")
    if len(parts) != 4:
        raise Exception(f"Invalid URN string: {urn_str}")
    if parts[0] != "urn":
        raise Exception(f"Invalid URN string: {urn_str}")
    resource_type, fqn_str = parts[3].split("/")
    fqn = parse_identifier(fqn_str, is_schema=(resource_type == "schema"))
    return URN(
        organization=parts[1],
        account_locator=parts[2],
        resource_type=resource_type,
        fqn=fqn,
    )
