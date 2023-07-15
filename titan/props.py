# Legacy
import re

Identifier = re.compile(r"[A-Za-z_][A-Za-z0-9_$]*")
# end legacy


import pyparsing as pp
from pyparsing import common

from .parseable_enum import ParseableEnum

# from pyparsing.common import convert_to_integer

Keyword = pp.CaselessKeyword
Literal = pp.CaselessLiteral

_Identifier = pp.Word(pp.alphanums + "_", pp.alphanums + "_$") | pp.dbl_quoted_string

Eq = Literal("=").suppress()
Lparen = Literal("(").suppress()
Rparen = Literal(")").suppress()

WITH = Keyword("WITH").suppress()
TAG = Keyword("TAG").suppress()
AS = Keyword("AS").suppress()

Boolean = Keyword("TRUE") | Keyword("FALSE")
Integer = pp.Word(pp.nums)

# Any = Keyword(pp.alphas)  # .set_results_name("any")
Any = pp.Word(pp.srange("[a-zA-Z0-9_]")) | pp.sgl_quoted_string  # pp.srange("[a-zA-Z_]"),


def parens(expr):
    return Lparen + expr + Rparen


def strip_quotes(tokens):
    return [tok.strip("'") for tok in tokens]


class Prop:
    def __init__(self, name, expression, value=None):
        self.name = name
        self.expression = expression
        self.value = value

    # TODO: investigate if this needs to exist or if add_condition is sufficient
    def validate(self, tokens):
        # TODO: use set name instead of first token
        prop_value = tokens[0]
        if self.value is None:
            return prop_value
        if not isinstance(prop_value, str):
            print("bad")
        res = self.value.parse_string(prop_value, parse_all=True)
        return res

    def render(self, value):
        if value is None:
            return ""
        return f"{self.name} = {value}"


class BoolProp(Prop):
    def __init__(self, name):
        expression = Keyword(name).suppress() + Eq + Any
        # This code fails if using add_parse_action because the parse action is applied every time a BoolProp is
        # initialized. This needs to be replaced with something safer
        value = (Keyword("TRUE") | Keyword("FALSE")).set_parse_action(lambda toks: toks[0].upper() == "TRUE")
        super().__init__(name, expression, value)

    def render(self, value):
        if value is None:
            return ""
        return f"{self.name} = {str(value).upper()}"


class IntProp(Prop):
    def __init__(self, name):
        expression = Keyword(name).suppress() + Eq + Any
        # replace with common.integer
        value = Integer().add_parse_action(common.convert_to_integer)
        super().__init__(name, expression, value)


class StringProp(Prop):
    def __init__(self, name, alt_tokens=[]):
        expression = Keyword(name).suppress() + Eq + Any
        value = pp.sgl_quoted_string | pp.one_of(alt_tokens, caseless=True, as_keyword=True)
        value = value.set_parse_action(strip_quotes)
        super().__init__(name, expression, value)

    def render(self, value):
        if value is None:
            return ""
        return f"{self.name} = '{value}'"


class FlagProp(Prop):
    def __init__(self, name):
        expression = pp.And(Keyword(part) for part in name.split(" "))
        super().__init__(name, expression)

    def validate(self, _):
        return True

    def render(self, value):
        return self.name.upper() if value else ""


class IdentifierProp(Prop):
    def __init__(self, name):
        expression = Keyword(name).suppress() + Eq + Any
        value = _Identifier | pp.sgl_quoted_string
        super().__init__(name, expression, value)

    # def validate(self, tokens):
    #     return True

    def render(self, value):
        if value is None:
            return ""
        return f"{self.name} = {value}"  # .fully_qualified_name


# class ResourceProp(Prop):
#     def __init__(self, name, resource_class):
#         pattern = Keyword(name).suppress() + Eq + _Identifier
#         super().__init__(name, pattern)
#         self.resource_class = resource_class

#     def validate(self, tokens):
#         return self.resource_class.all[tokens[-1]]

#     def render(self, value):
#         if value is None:
#             return ""
#         return f"{self.name} = {value.fully_qualified_name}"


class StringListProp(Prop):
    def __init__(self, name):
        expression = (
            Keyword(name).suppress() + Eq + parens(common.comma_separated_list).add_parse_action(strip_quotes)
        )
        super().__init__(name, expression)

    def render(self, values):
        if values:
            strings = ", ".join([f"'{item}'" for item in values])
            return f"{self.name} = ({strings})"
        else:
            return ""


class PropSet(Prop):
    def __init__(self, name, expected_props):
        expression = (
            Keyword(name).suppress() + Eq + pp.Combine(pp.nested_expr(), adjacent=False, join_string=" ")
        )
        super().__init__(name, expression)
        self.expected_props = expected_props

    # def validate(self, tokens):
    #     # return [tok.strip("'") for tok in tokens]
    #     raise NotImplementedError

    # def normalize(self, value: str) -> Any:
    #     normalized = {}
    #     for name, prop in self.expected_props.items():
    #         match = prop.search(value)
    #         if match:
    #             normalized[name.lower()] = match
    #     return normalized if normalized else None

    def render(self, values):
        if values is None or len(values) == 0:
            return ""
        kv_pairs = []
        for name, prop in self.expected_props.items():
            if name.lower() in values:
                kv_pairs.append(prop.render(values[name.lower()]))

        return f"{self.name} = ({', '.join(kv_pairs)})"


class TagsProp(Prop):
    """
    [ [ WITH ] TAG ( <tag_name> = '<tag_value>' [ , <tag_name> = '<tag_value>' , ... ] ) ]
    """

    def __init__(self):
        name = "TAGS"
        expression = (
            WITH + TAG + pp.nested_expr(content=pp.delimited_list(_Identifier + Eq + pp.sgl_quoted_string))
        )
        value = None
        super().__init__(name, expression, value)

    def validate(self, tokens):
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


class IdentifierListProp(Prop):
    def __init__(self, name, naked=False):
        if naked:
            # This might need to be Any instead of _Identifier
            expression = Keyword(name).suppress() + pp.Group(pp.delimited_list(_Identifier))
        else:
            expression = Keyword(name).suppress() + Eq + parens(pp.Group(pp.delimited_list(_Identifier)))
        value = None  # TODO: validate function should turn identifiers into objects
        super().__init__(name, expression, value)

    def validate(self, tokens):
        return tokens.as_list()

    def render(self, value):
        if value:
            # tag_kv_pairs = ", ".join([f"{key} = '{value}'" for key, value in value.items()])
            # TODO: wtf is this
            identifiers = ", ".join([str(id) for id in value])
            return f"{self.name} = ({identifiers})"
        else:
            return ""


class EnumProp(Prop):
    def __init__(self, name, enum_or_list):
        # enum_or_list: a single enum class or a list of valid enum values
        valid_values = set(enum_or_list)
        expression = Keyword(name).suppress() + Eq + Any().add_parse_action(strip_quotes)
        value = pp.one_of([e.value for e in valid_values], caseless=True, as_keyword=True)

        super().__init__(name, expression, value)
        self.enum_type = type(enum_or_list[0]) if isinstance(enum_or_list, list) else enum_or_list

    def validate(self, tokens):
        return self.enum_type.parse(tokens[0])

    def render(self, value):
        if value is None:
            return ""
        return f"{self.name} = {value.value}"


class QueryProp(Prop):
    def __init__(self, name):
        expression = AS + pp.Word(pp.printables + " \n")
        super().__init__(name, expression)

    def render(self, value):
        if value is None:
            return ""
        return f"{self.name} {value}"


# def prop_scan(resource_type, props, sql):


class Props:
    def __init__(self, **props):
        self.props = props

    def parse(self, sql):
        resource_type = "Foo"
        if sql.strip() == "":
            return {}
        lexicon = []
        for prop_kwarg, prop_or_list in self.props.items():
            if isinstance(prop_or_list, list):
                prop_list = prop_or_list
                # raise Exception("no longer supported")
            else:
                prop_list = [prop_or_list]
            for prop in prop_list:
                # https://docs.python.org/3/faq/programming.html#why-do-lambdas-defined-in-a-loop-with-different-values-all-return-the-same-result
                named_marker = pp.Empty().set_parse_action(
                    lambda s, loc, toks, name=prop_kwarg.lower(): (name, loc)
                )
                lexicon.append(prop.expression.set_parse_action(prop.validate) + named_marker)

        parser = pp.MatchFirst(lexicon).ignore(pp.c_style_comment)
        # ppt = pp.testing
        # print("-" * 80)
        # print(ppt.with_line_numbers(sql))
        # print("-" * 80)

        found_props = {}
        remainder_sql = sql
        while True:
            try:
                tokens, (prop_kwarg, end_index) = parser.parse_string(remainder_sql)
            except pp.ParseException:
                print(remainder_sql)
                # TODO: better error messages
                raise Exception(
                    f"Failed to parse {resource_type} props [{remainder_sql.strip().splitlines()[0]}]"
                )

            # TODO: this should be some sort of recursive/nested thing
            # if isinstance(found_prop, PropSet):
            # if prop_kwarg == "encryption":
            #     tokens = prop_scan(resource_type, self.props[prop_kwarg.upper()].expected_props, tokens)
            #     print(prop_kwarg)

            found_props[prop_kwarg] = tokens
            remainder_sql = remainder_sql[end_index:]
            if remainder_sql.strip() == "":
                break

        if len(remainder_sql.strip()) > 0:
            raise Exception(f"Failed to parse props: {remainder_sql}")
        return found_props


class FileFormatProp(Prop):
    def __init__(self, name):
        expression = (
            Keyword(name).suppress()
            + Eq
            + parens(StringProp("FORMAT_NAME").expression)  # | AnonFileFormatProp().expression)
        )
        value = None
        super().__init__(name, expression, value)

    def render(self, value):
        if value is None:
            return ""
        return f"{self.name} = ({value.sql})"


class ExpressionProp(Prop):
    def __init__(self, name):
        expression = Keyword(name).suppress() + ... + Keyword("AS").suppress()
        # + Eq + Any()
        value = None
        super().__init__(name, expression, value)

    def render(self, value):
        if value is None:
            return ""
        return f"{self.name} = {value.sql}"


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
    QUOTED_IDENTIFIERS_IGNORE_CASE = "QUOTED_IDENTIFIERS_IGNORE_CASE"
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
