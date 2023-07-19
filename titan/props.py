import pyparsing as pp
from pyparsing import common

from .parseable_enum import ParseableEnum

Keyword = pp.CaselessKeyword
Literal = pp.CaselessLiteral

Identifier = pp.Word(pp.alphanums + "_", pp.alphanums + "_$") | pp.dbl_quoted_string

Eq = Literal("=").suppress()
Lparen = Literal("(").suppress()
Rparen = Literal(")").suppress()

WITH = Keyword("WITH").suppress()
TAG = Keyword("TAG").suppress()
AS = Keyword("AS").suppress()

Boolean = Keyword("TRUE") | Keyword("FALSE")
Integer = pp.Word(pp.nums)

Any = pp.Word(pp.srange("[a-zA-Z0-9_]")) | pp.sgl_quoted_string
# Expression


def parens(expr):
    return Lparen + expr + Rparen


def strip_quotes(tokens):
    return [tok.strip("'") for tok in tokens]


class Prop:
    def __init__(self, name, expression, alt_tokens=[]):
        self.name = name
        self.expression = expression
        self.alt_tokens = set([tok.lower() for tok in alt_tokens])

    def parse(self, sql):
        try:
            return self.on_parse(self.expression.parse_string(sql))
        except pp.ParseException:
            return None

    def on_parse(self, tokens):
        if len(tokens) > 1:
            raise Exception(f"Too many tokens: {tokens}")
        prop_value = tokens[0]
        if self.alt_tokens and prop_value.lower() in self.alt_tokens:
            return prop_value
        try:
            return self.validate(prop_value)
        except:
            raise Exception(f"Failure to validate {self} => [{prop_value}]")

    def validate(self, prop_value):
        # return None
        raise NotImplementedError

    # def render(self, value):
    #     if value is None:
    #         return ""
    #     return f"{self.name} = {value}"


class BoolProp(Prop):
    def __init__(self, name, **kwargs):
        expression = Keyword(name).suppress() + Eq + Any
        super().__init__(name, expression, **kwargs)

    def validate(self, prop_value):
        if prop_value.lower() not in ["true", "false"]:
            raise ValueError(f"Invalid boolean value: {prop_value}")
        return prop_value.lower() == "true"

    # def render(self, value):
    #     if value is None:
    #         return ""
    #     return f"{self.name} = {str(value).upper()}"


class IntProp(Prop):
    def __init__(self, name, **kwargs):
        expression = Keyword(name).suppress() + Eq + Any
        super().__init__(name, expression, **kwargs)

    def validate(self, prop_value):
        try:
            return int(prop_value)
        except ValueError:
            raise ValueError(f"Invalid integer value: {prop_value}")


class StringProp(Prop):
    def __init__(self, name, **kwargs):
        expression = Keyword(name).suppress() + pp.Opt(Eq) + Any
        super().__init__(name, expression, **kwargs)

    def validate(self, prop_value):
        return prop_value.strip("'")

    # def render(self, value):
    #     if value is None:
    #         return ""
    #     return f"{self.name} = '{value}'"


class FlagProp(Prop):
    def __init__(self, name):
        expression = pp.Combine(
            pp.And(Keyword(part) for part in name.split(" ")), adjacent=False, join_string=" "
        )
        super().__init__(name, expression)

    def validate(self, _):
        return True

    # def render(self, value):
    #     return self.name.upper() if value else ""


class IdentifierProp(Prop):
    def __init__(self, name, resource_class):
        expression = Keyword(name).suppress() + Eq + Any
        super().__init__(name, expression)
        self.resource_class = resource_class

    def validate(self, prop_value):
        return self.resource_class.find(prop_value.strip("'"))

    # def render(self, value):
    #     if value is None:
    #         return ""
    #     return f"{self.name} = {value}"  # .fully_qualified_name


class StringListProp(Prop):
    def __init__(self, name):
        expression = Keyword(name).suppress() + Eq + Lparen + ... + Rparen
        super().__init__(name, expression)

    def validate(self, prop_value):
        return [[tok.strip(" '") for tok in prop_value.split(",")]]

    # def render(self, values):
    #     if values:
    #         strings = ", ".join([f"'{item}'" for item in values])
    #         return f"{self.name} = ({strings})"
    #     else:
    #         return ""


class PropSet(Prop):
    def __init__(self, name, props):
        expression = Keyword(name).suppress() + Eq + Lparen + ... + Rparen
        super().__init__(name, expression)
        self.props = props

    def validate(self, prop_value):
        return self.props.parse(prop_value)

    # def render(self, values):
    #     if values is None or len(values) == 0:
    #         return ""
    #     kv_pairs = []
    #     for name, prop in self.expected_props.items():
    #         if name.lower() in values:
    #             kv_pairs.append(prop.render(values[name.lower()]))

    #     return f"{self.name} = ({', '.join(kv_pairs)})"


class TagsProp(Prop):
    """
    [ [ WITH ] TAG ( <tag_name> = '<tag_value>' [ , <tag_name> = '<tag_value>' , ... ] ) ]
    """

    def __init__(self):
        # pp.nested_expr(content=pp.delimited_list(Identifier + Eq + pp.sgl_quoted_string))
        name = "TAGS"
        expression = WITH + TAG + Lparen + ... + Rparen
        super().__init__(name, expression)

    def validate(self, prop_value):
        raise NotImplementedError

    # def normalize(self, value: str) -> Any:
    #     tag_matches = re.findall(self.tag_pattern, value)
    #     return {key: value for key, value in tag_matches}

    def render(self, value: Any) -> str:
        if value:
            tag_kv_pairs = ", ".join([f"{key} = '{value}'" for key, value in value.items()])
            return f"WITH TAG ({tag_kv_pairs})"
        else:
            return ""


class DictProp(Prop):
    """
    HEADERS = ( '<header_1>' = '<value_1>' [ , '<header_2>' = '<value_2>' ... ] )
    """

    def __init__(self, name):
        expression = (
            Keyword(name).suppress()
            + pp.Opt(Eq)
            + pp.nested_expr(content=pp.delimited_list(pp.sgl_quoted_string + Eq + pp.sgl_quoted_string))
        )
        super().__init__(name, expression)

    def validate(self, prop_value):
        return prop_value

    # def validate(self, tokens):
    #     return self.enum_type.parse(tokens[0])

    # def render(self, value: Any) -> str:
    #     if value:
    #         tag_kv_pairs = ", ".join([f"{key} = '{value}'" for key, value in value.items()])
    #         return f"{self.name} = ({tag_kv_pairs})"
    #     else:
    #         return ""


# class IdentifierListProp(Prop):
#     def __init__(self, name, naked=False):
#         if naked:
#             # This might need to be Any instead of Identifier
#             expression = Keyword(name).suppress() + pp.Group(pp.delimited_list(Identifier))
#         else:
#             expression = Keyword(name).suppress() + Eq + parens(pp.Group(pp.delimited_list(Identifier)))
#         # value = None
#         super().__init__(name, expression)

#     # TODO: validate function should turn identifiers into objects
#     def validate(self, prop_value):
#         # return tokens.as_list()
#         raise NotImplementedError

#     def render(self, value):
#         if value:
#             # tag_kv_pairs = ", ".join([f"{key} = '{value}'" for key, value in value.items()])
#             # TODO: wtf is this
#             identifiers = ", ".join([str(id) for id in value])
#             return f"{self.name} = ({identifiers})"
#         else:
#             return ""


class EnumProp(Prop):
    def __init__(self, name, enum_or_list):
        expression = Keyword(name).suppress() + pp.Opt(Eq) + Any
        super().__init__(name, expression)
        self.enum_type = type(enum_or_list[0]) if isinstance(enum_or_list, list) else enum_or_list
        self.valid_values = set(enum_or_list)

    def validate(self, prop_value):
        parsed = self.enum_type.parse(prop_value.strip("'"))
        if parsed not in self.valid_values:
            raise ValueError(f"Invalid value: {prop_value} must be one of {self.valid_values}")
        return parsed

    def render(self, value):
        if value is None:
            return ""
        return f"{self.name} = {value.value}"


class QueryProp(Prop):
    def __init__(self, name):
        expression = AS + pp.Word(pp.printables + " \n")
        super().__init__(name, expression)

    def validate(self, prop_value):
        return prop_value

    # def render(self, value):
    #     if value is None:
    #         return ""
    #     return f"{self.name} {value}"


# class ColumnsProp(Prop):
#     def __init__(self, name, enum_or_list):
#         valid_values = set(enum_or_list)
#         column_type = pp.one_of([e.value for e in valid_values], caseless=True, as_keyword=True)
#         column = Identifier + column_type
#         expression = Lparen + ... + Rparen
#         # value = pp.one_of([e.value for e in valid_values], caseless=True, as_keyword=True)
#         value = pp.delimited_list(column)
#         super().__init__(name, expression, value)

#     def validate(self, tokens):
#         return list([tok.split() for tok in tokens])

#     # def render(self, values):
#     #     if values is None:
#     #         return ""
#     #     return f"({', '.join([ col + ' ' + col_type for col, col_type in values])})"


class ResourceListProp(Prop):
    def __init__(self, resource_class):
        expression = Lparen() + ... + Rparen()
        super().__init__(name=None, expression=expression)
        self.resource_class = resource_class

    def validate(self, prop_value):
        resource_list = []
        for tok in common.comma_separated_list.parse_string(prop_value):
            try:
                res = self.resource_class.from_sql(tok.strip())
            except:
                raise Exception(f"Failed to parse {self.resource_class} [{tok.strip()}]")
            resource_list.append(res)
        return resource_list

    # def render(self, values):
    #     if values is None:
    #         return ""
    #     return f"({', '.join([ col + ' ' + col_type for col, col_type in values])})"


class ExpressionProp(Prop):
    def __init__(self, name):
        expression = Keyword(name).suppress() + ... + pp.FollowedBy(Keyword("AS"))
        super().__init__(name, expression)

    def validate(self, prop_value):
        return prop_value.strip()

    # def render(self, value):
    #     if value is None:
    #         return ""
    #     return f"{self.name} = {value.sql}"


class Props:
    def __init__(self, _name: str = None, _start_token: str = None, **props):
        self.props = props
        self.name = _name
        self.start_token = pp.MatchFirst(Keyword(_start_token)) if _start_token else pp.Empty()

    def parse(self, sql):
        # Instead of passing in resource type, just bubble up exceptions with the important metadata
        # resource_type = "Foo"
        if sql.strip() == "":
            return {}

        lexicon = []
        for prop_kwarg, prop in self.props.items():
            # https://docs.python.org/3/faq/programming.html#why-do-lambdas-defined-in-a-loop-with-different-values-all-return-the-same-result
            named_marker = pp.Empty().set_parse_action(
                lambda s, loc, toks, name=prop_kwarg.lower(): (name, loc)
            )
            lexicon.append(prop.expression.set_parse_action(prop.on_parse) + named_marker)

        parser = pp.MatchFirst(lexicon).ignore(pp.c_style_comment)
        found_props = {}
        remainder_sql = self.consume_start_token(sql)
        while True:
            try:
                tokens, (prop_kwarg, end_index) = parser.parse_string(remainder_sql)
            except pp.ParseException:
                print(remainder_sql)
                # TODO: better error messages
                raise Exception(f"Failed to parse props [{remainder_sql.strip().splitlines()[0]}]")

            found_props[prop_kwarg] = tokens
            remainder_sql = remainder_sql[end_index:].strip()
            if remainder_sql == "":
                break

        if len(remainder_sql) > 0:
            raise Exception(f"Unparsed props remain: [{remainder_sql}]")
        return found_props

    def consume_start_token(self, sql):
        for toks, _, end in self.start_token.scan_string(sql):
            if toks:
                return sql[end:]
        return sql


class SessionParameter(ParseableEnum):
    ABORT_DETACHED_QUERY = "ABORT_DETACHED_QUERY"
    AUTOCOMMIT = "AUTOCOMMIT"
    AUTOCOMMIT_API_SUPPORTED = "AUTOCOMMIT_API_SUPPORTED"
    BINARY_INPUT_FORMAT = "BINARY_INPUT_FORMAT"
    BINARY_OUTPUT_FORMAT = "BINARY_OUTPUT_FORMAT"
    CLIENT_ENABLE_CONSERVATIVE_MEMORY_USAGE = "CLIENT_ENABLE_CONSERVATIVE_MEMORY_USAGE"
    CLIENT_ENABLE_DEFAULT_OVERWRITE_IN_PUT = "CLIENT_ENABLE_DEFAULT_OVERWRITE_IN_PUT"
    CLIENT_ENABLE_LOG_INFO_STATEMENT_PARAMETERS = "CLIENT_ENABLE_LOG_INFO_STATEMENT_PARAMETERS"
    CLIENT_MEMORY_LIMIT = "CLIENT_MEMORY_LIMIT"
    CLIENT_METADATA_REQUEST_USE_CONNECTION_CTX = "CLIENT_METADATA_REQUEST_USE_CONNECTION_CTX"
    CLIENT_METADATA_USE_SESSION_DATABASE = "CLIENT_METADATA_USE_SESSION_DATABASE"
    CLIENT_PREFETCH_THREADS = "CLIENT_PREFETCH_THREADS"
    CLIENT_RESULT_CHUNK_SIZE = "CLIENT_RESULT_CHUNK_SIZE"
    CLIENT_RESULT_COLUMN_CASE_INSENSITIVE = "CLIENT_RESULT_COLUMN_CASE_INSENSITIVE"
    CLIENT_SESSION_CLONE = "CLIENT_SESSION_CLONE"
    CLIENT_SESSION_KEEP_ALIVE = "CLIENT_SESSION_KEEP_ALIVE"
    CLIENT_SESSION_KEEP_ALIVE_HEARTBEAT_FREQUENCY = "CLIENT_SESSION_KEEP_ALIVE_HEARTBEAT_FREQUENCY"
    CLIENT_TIMESTAMP_TYPE_MAPPING = "CLIENT_TIMESTAMP_TYPE_MAPPING"
    CSV_TIMESTAMP_FORMAT = "CSV_TIMESTAMP_FORMAT"
    C_API_QUERY_RESULT_FORMAT = "C_API_QUERY_RESULT_FORMAT"
    DATE_INPUT_FORMAT = "DATE_INPUT_FORMAT"
    DATE_OUTPUT_FORMAT = "DATE_OUTPUT_FORMAT"
    ENABLE_CONSOLE_OUTPUT = "ENABLE_CONSOLE_OUTPUT"
    ENABLE_UNLOAD_PHYSICAL_TYPE_OPTIMIZATION = "ENABLE_UNLOAD_PHYSICAL_TYPE_OPTIMIZATION"
    ERROR_ON_NONDETERMINISTIC_MERGE = "ERROR_ON_NONDETERMINISTIC_MERGE"
    ERROR_ON_NONDETERMINISTIC_UPDATE = "ERROR_ON_NONDETERMINISTIC_UPDATE"
    GEOGRAPHY_OUTPUT_FORMAT = "GEOGRAPHY_OUTPUT_FORMAT"
    GEOMETRY_OUTPUT_FORMAT = "GEOMETRY_OUTPUT_FORMAT"
    GO_QUERY_RESULT_FORMAT = "GO_QUERY_RESULT_FORMAT"
    JDBC_FORMAT_DATE_WITH_TIMEZONE = "JDBC_FORMAT_DATE_WITH_TIMEZONE"
    JDBC_QUERY_RESULT_FORMAT = "JDBC_QUERY_RESULT_FORMAT"
    JDBC_TREAT_DECIMAL_AS_INT = "JDBC_TREAT_DECIMAL_AS_INT"
    JDBC_TREAT_TIMESTAMP_NTZ_AS_UTC = "JDBC_TREAT_TIMESTAMP_NTZ_AS_UTC"
    JDBC_USE_SESSION_TIMEZONE = "JDBC_USE_SESSION_TIMEZONE"
    JSON_INDENT = "JSON_INDENT"
    JS_TREAT_INTEGER_AS_BIGINT = "JS_TREAT_INTEGER_AS_BIGINT"
    LANGUAGE = "LANGUAGE"
    LOCK_TIMEOUT = "LOCK_TIMEOUT"
    LOG_LEVEL = "LOG_LEVEL"
    MULTI_STATEMENT_COUNT = "MULTI_STATEMENT_COUNT"
    ODBC_QUERY_RESULT_FORMAT = "ODBC_QUERY_RESULT_FORMAT"
    ODBC_SCHEMA_CACHING = "ODBC_SCHEMA_CACHING"
    ODBC_USE_CUSTOM_SQL_DATA_TYPES = "ODBC_USE_CUSTOM_SQL_DATA_TYPES"
    PREVENT_UNLOAD_TO_INTERNAL_STAGES = "PREVENT_UNLOAD_TO_INTERNAL_STAGES"
    PYTHON_CONNECTOR_QUERY_RESULT_FORMAT = "PYTHON_CONNECTOR_QUERY_RESULT_FORMAT"
    PYTHON_SNOWPARK_USE_SCOPED_TEMP_OBJECTS = "PYTHON_SNOWPARK_USE_SCOPED_TEMP_OBJECTS"
    PYTHON_SNOWPARK_USE_SQL_SIMPLIFIER = "PYTHON_SNOWPARK_USE_SQL_SIMPLIFIER"
    QA_TEST_NAME = "QA_TEST_NAME"
    QUERY_RESULT_FORMAT = "QUERY_RESULT_FORMAT"
    QUERY_TAG = "QUERY_TAG"
    QUOTEDIdentifierS_IGNORE_CASE = "QUOTEDIdentifierS_IGNORE_CASE"
    READ_LATEST_WRITES = "READ_LATEST_WRITES"
    ROWS_PER_RESULTSET = "ROWS_PER_RESULTSET"
    S3_STAGE_VPCE_DNS_NAME = "S3_STAGE_VPCE_DNS_NAME"
    SEARCH_PATH = "SEARCH_PATH"
    SHOW_EXTERNAL_TABLE_KIND_AS_TABLE = "SHOW_EXTERNAL_TABLE_KIND_AS_TABLE"
    SIMULATED_DATA_SHARING_CONSUMER = "SIMULATED_DATA_SHARING_CONSUMER"
    SNOWPARK_HIDE_INTERNAL_ALIAS = "SNOWPARK_HIDE_INTERNAL_ALIAS"
    SNOWPARK_LAZY_ANALYSIS = "SNOWPARK_LAZY_ANALYSIS"
    SNOWPARK_REQUEST_TIMEOUT_IN_SECONDS = "SNOWPARK_REQUEST_TIMEOUT_IN_SECONDS"
    SNOWPARK_STORED_PROC_IS_FINAL_TABLE_QUERY = "SNOWPARK_STORED_PROC_IS_FINAL_TABLE_QUERY"
    SNOWPARK_USE_SCOPED_TEMP_OBJECTS = "SNOWPARK_USE_SCOPED_TEMP_OBJECTS"
    SQL_API_NULLABLE_IN_RESULT_SET = "SQL_API_NULLABLE_IN_RESULT_SET"
    SQL_API_QUERY_RESULT_FORMAT = "SQL_API_QUERY_RESULT_FORMAT"
    STATEMENT_QUEUED_TIMEOUT_IN_SECONDS = "STATEMENT_QUEUED_TIMEOUT_IN_SECONDS"
    STATEMENT_TIMEOUT_IN_SECONDS = "STATEMENT_TIMEOUT_IN_SECONDS"
    STRICT_JSON_OUTPUT = "STRICT_JSON_OUTPUT"
    TIMESTAMP_DAY_IS_ALWAYS_24H = "TIMESTAMP_DAY_IS_ALWAYS_24H"
    TIMESTAMP_INPUT_FORMAT = "TIMESTAMP_INPUT_FORMAT"
    TIMESTAMP_LTZ_OUTPUT_FORMAT = "TIMESTAMP_LTZ_OUTPUT_FORMAT"
    TIMESTAMP_NTZ_OUTPUT_FORMAT = "TIMESTAMP_NTZ_OUTPUT_FORMAT"
    TIMESTAMP_OUTPUT_FORMAT = "TIMESTAMP_OUTPUT_FORMAT"
    TIMESTAMP_TYPE_MAPPING = "TIMESTAMP_TYPE_MAPPING"
    TIMESTAMP_TZ_OUTPUT_FORMAT = "TIMESTAMP_TZ_OUTPUT_FORMAT"
    TIMEZONE = "TIMEZONE"
    TIME_INPUT_FORMAT = "TIME_INPUT_FORMAT"
    TIME_OUTPUT_FORMAT = "TIME_OUTPUT_FORMAT"
    TRACE_LEVEL = "TRACE_LEVEL"
    TRANSACTION_ABORT_ON_ERROR = "TRANSACTION_ABORT_ON_ERROR"
    TRANSACTION_DEFAULT_ISOLATION_LEVEL = "TRANSACTION_DEFAULT_ISOLATION_LEVEL"
    TWO_DIGIT_CENTURY_START = "TWO_DIGIT_CENTURY_START"
    UI_QUERY_RESULT_FORMAT = "UI_QUERY_RESULT_FORMAT"
    UNSUPPORTED_DDL_ACTION = "UNSUPPORTED_DDL_ACTION"
    USE_CACHED_RESULT = "USE_CACHED_RESULT"
    WEEK_OF_YEAR_POLICY = "WEEK_OF_YEAR_POLICY"
    WEEK_START = "WEEK_START"


# SESSION_PARAMETERS = {
#     param.value.lower(): value for param in set(SessionParameter)
#     }


class SessionParametersProp(Prop):
    pass
