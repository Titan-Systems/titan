# Legacy
import re

Identifier = re.compile(r"[A-Za-z_][A-Za-z0-9_$]*")
# end legacy


import pyparsing as pp
from pyparsing import common

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
Any = pp.Word(pp.srange("[a-zA-Z0-9_]"))  # pp.srange("[a-zA-Z_]"),


def parens(expr):
    return Lparen + expr + Rparen


def strip_quotes(tokens):
    return [tok.strip("'") for tok in tokens]


class Prop:
    def __init__(self, name, expression, value=None, valid_tokens=[]):
        self.name = name
        self.expression = expression
        self.value = value
        self.valid_tokens = valid_tokens
        self.expression  # .add_parse_action(self.validate)

    # TODO: investigate if this needs to exist or if add_condition is sufficient
    def validate(self, tokens):
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
        # value = Boolean.set_parse_action(lambda toks: toks[0].upper() == "TRUE")
        value = (Keyword("TRUE") | Keyword("FALSE")).set_parse_action(lambda toks: toks[0].upper() == "TRUE")
        super().__init__(name, expression, value)

    # def validate(self, tokens):
    #     return super().validate(tokens)

    def render(self, value):
        if value is None:
            return ""
        return f"{self.name} = {str(value).upper()}"


class IntProp(Prop):
    def __init__(self, name):
        expression = Keyword(name).suppress() + Eq + Any  # pp.Word(pp.nums)
        # replace with common.integer
        value = Integer.add_parse_action(common.convert_to_integer)
        super().__init__(name, expression, value)


class StringProp(Prop):
    def __init__(self, name, valid_values=[], alt_tokens=[]):
        expression = Keyword(name).suppress() + Eq + pp.quoted_string  # .add_parse_action(pp.remove_quotes)
        value = None
        super().__init__(name, expression, value)
        # self.valid_values = valid_values

    # def normalize(self, value):
    #     return value.strip("'")

    def render(self, value):
        if value is None:
            return ""
        return f"{self.name} = '{value}'"


class FlagProp(Prop):
    def __init__(self, name):
        expression = Keyword(name)
        super().__init__(name, expression)

    def validate(self, _):
        return True

    def render(self, value):
        return self.name.upper() if value else ""


class IdentifierProp(Prop):
    def __init__(self, name):
        expression = Keyword(name).suppress() + Eq + _Identifier
        super().__init__(name, expression)

    def render(self, value):
        if value is None:
            return ""
        return f"{self.name} = {value.fully_qualified_name}"


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
            Keyword(name).suppress() + Eq + parens(common.comma_separated_list).add_parse_action(pp.remove_quotes)
        )
        super().__init__(name, expression)

    def validate(self, tokens):
        # return [tok.strip("'") for tok in tokens]
        return tokens

    def render(self, values):
        if values:
            strings = ", ".join([f"'{item}'" for item in values])
            return f"{self.name} = ({strings})"
        else:
            return ""


# "DIRECTORY": PropList(
#     "DIRECTORY", {"ENABLE": BoolProp("ENABLE"), "REFRESH_ON_CREATE": BoolProp("REFRESH_ON_CREATE")}
# ),

# directoryTableParams (for internal stages) ::=
#   [ DIRECTORY = ( ENABLE = { TRUE | FALSE }
#                   [ REFRESH_ON_CREATE =  { TRUE | FALSE } ] ) ]


class PropList(Prop):
    def __init__(self, name, expected_props):
        # super().__init__(name, rf"{name}\s*=\s*\((.*)\)")
        self.expected_props = expected_props
        props = [prop.expression for prop in expected_props.values()]
        # expression = Keyword(name).suppress() + Eq + pp.nested_expr(content=pp.OneOrMore(props))
        # expression = Keyword(name).suppress() + Eq + pp.nested_expr(content=pp.one_of(props, caseless=True, as_keyword=True))
        # expression = Any
        expression = Keyword(name).suppress() + Eq + pp.Combine(pp.nested_expr(), adjacent=False, join_string=" ")
        super().__init__(name, expression)

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
        expression = WITH + TAG + pp.nested_expr(content=pp.delimited_list(_Identifier + Eq + pp.sgl_quoted_string))
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
    def __init__(self, name):
        # pp.nested_expr(content=_Identifier)
        # parens(common.comma_separated_list)
        expression = Keyword(name).suppress() + Eq + parens(pp.Group(pp.delimited_list(_Identifier)))
        value = None  # TODO: validate function should turn identifiers into objects
        super().__init__(name, expression, value)

    # def normalize(self, value):
    #     identifier_matches = re.findall(Identifier.pattern, value)
    #     return [match for match in identifier_matches]

    def validate(self, tokens):
        return tokens.as_list()
        # return super().validate(tokens)

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
        # (Any | pp.sgl_quoted_string).add_parse_action(pp.remove_quotes)
        expression = Keyword(name).suppress() + Eq + (Any | pp.sgl_quoted_string).add_parse_action(strip_quotes)
        value = pp.one_of([e.value for e in valid_values], caseless=True, as_keyword=True)
        super().__init__(name, expression, value)
        self.enum_type = type(enum_or_list[0]) if isinstance(enum_or_list, list) else enum_or_list
        # self.valid_values = set(self.enum_type)

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

    # def normalize(self, value: str) -> str:
    #     return value.strip("'").upper()

    def render(self, value):
        if value is None:
            return ""
        return f"{self.name} {value}"


def prop_scan(props, sql):
    if sql.strip() == "":
        return {}
    lexicon = []
    for prop_kwarg, prop_or_list in props.items():
        if isinstance(prop_or_list, list):
            prop_list = prop_or_list
        else:
            prop_list = [prop_or_list]
        for prop in prop_list:
            # https://docs.python.org/3/faq/programming.html#why-do-lambdas-defined-in-a-loop-with-different-values-all-return-the-same-result
            named_marker = pp.Empty().set_parse_action(lambda s, loc, toks, name=prop_kwarg.lower(): (name, loc))
            lexicon.append(prop.expression.set_parse_action(prop.validate) + named_marker)  #

    remainder_sql = sql
    parser = pp.MatchFirst(lexicon)
    ppt = pp.testing
    print("-" * 80)
    print(ppt.with_line_numbers(sql))
    print("-" * 80)
    print(parser)

    found_props = {}

    while True:
        try:
            tokens, (prop_kwarg, end_index) = parser.parse_string(remainder_sql)
        except pp.ParseException:
            print(remainder_sql)
            raise Exception(f"Failed to parse props: {remainder_sql}")
        # except Exception:
        #     print("wtf")

        if prop_kwarg == "encryption":
            # print(prop_kwarg)
            tokens = prop_scan(props[prop_kwarg.upper()].expected_props, tokens)
            print(prop_kwarg)

        found_props[prop_kwarg] = tokens
        print(prop_kwarg, "->", repr(tokens))
        remainder_sql = remainder_sql[end_index:]
        if remainder_sql.strip() == "":
            break

    if len(remainder_sql.strip()) > 0:
        raise Exception(f"Failed to parse props: {remainder_sql}")
    return found_props
