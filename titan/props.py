from abc import ABC

import pyparsing as pp
from .enums import DataType
from .parse import (
    _consume_tokens,
    Keyword,
    Keywords,
    Literals,
    list_expr,
    parens,
    _best_guess_failing_parser,
    Identifier,
    FullyQualifiedIdentifier,
    LPAREN,
    RPAREN,
    EQUALS,
    ARROW,
    ANY,
)


def strip_quotes(tokens):
    return [tok.strip("'") for tok in tokens]


class Prop(ABC):
    """
    A Prop is a named expression that can be parsed from a SQL string.
    """

    def __init__(self, label, value_expr=ANY(), eq=True, parens=False, alt_tokens=[]):
        self.label = label
        self.eq = eq
        self.alt_tokens = set([tok.lower() for tok in alt_tokens])
        eq_expr = EQUALS() if eq else pp.Empty()

        if parens:
            value_expr = list_expr(value_expr)

        self.expr = Keywords(self.label).suppress() + eq_expr.suppress() + value_expr

    def __repr__(self):
        return f"{self.__class__.__name__}('{self.label}')"

    def parse(self, sql):
        try:
            values = self.expr.parse_string(sql).as_list()
            return self.on_parse(values)
        except pp.ParseException:
            return None

    def on_parse(self, values):
        if len(values) > 1:
            raise Exception(f"Too many values: {values}")
        if len(values) == 0:
            raise Exception("No values parsed")
        prop_value = values[0]
        if self.alt_tokens and prop_value.lower() in self.alt_tokens:
            return prop_value
        return self.validate(prop_value)

    def validate(self, prop_value):
        raise NotImplementedError

    def render(self, value):
        raise NotImplementedError


class Props:
    def __init__(self, _name: str = None, _start_token: str = None, **props):
        self.props = props
        self.name = _name
        self.start_token = Literals(_start_token) if _start_token else pp.Empty()

    def parse(self, sql):
        if sql.strip() == "":
            return {}

        lexicon = []
        for prop_kwarg, prop in self.props.items():
            # https://docs.python.org/3/faq/programming.html#why-do-lambdas-defined-in-a-loop-with-different-values-all-return-the-same-result
            named_marker = pp.Empty().set_parse_action(lambda s, loc, toks, name=prop_kwarg.lower(): (name, loc))
            lexicon.append(prop.expr.set_parse_action(prop.on_parse) + named_marker)

        parser = pp.MatchFirst(lexicon).ignore(pp.c_style_comment)
        found_props = {}
        remainder_sql = _consume_tokens(self.start_token, sql)
        while True:
            try:
                tokens, (prop_kwarg, end_index) = parser.parse_string(remainder_sql)
            except pp.ParseException:
                formatted_sql = "\n".join(["  " + line.strip() for line in remainder_sql.splitlines()])
                # formatted_parser = _format_parser(parser)
                failing_parser = _best_guess_failing_parser(parser, remainder_sql)
                raise Exception(
                    f"Failed to parse props.\nSQL: \n```\n{formatted_sql}\n```\n\nParser:\n{failing_parser}\n\n"
                )

            found_props[prop_kwarg] = tokens
            remainder_sql = remainder_sql[end_index:].strip()
            if remainder_sql == "":
                break

        if len(remainder_sql) > 0:
            raise Exception(f"Unparsed props remain: [{remainder_sql}]")
        return found_props

    def render(self, values):
        pass


class BoolProp(Prop):
    def validate(self, prop_value):
        if prop_value.lower() not in ["true", "false"]:
            raise ValueError(f"Invalid boolean value: {prop_value}")
        return prop_value.lower() == "true"

    def render(self, value):
        if value is None:
            return ""
        eq = " = " if self.eq else " "
        return f"{self.label}{eq}{str(value).upper()}"


class IntProp(Prop):
    def validate(self, prop_value):
        try:
            return int(prop_value)
        except ValueError:
            raise ValueError(f"Invalid integer value: {prop_value}")

    def render(self, value):
        if value is None:
            return ""
        eq = " = " if self.eq else " "
        return f"{self.label}{eq}{value}"


class StringProp(Prop):
    def validate(self, prop_value):
        return prop_value.strip("'")

    def render(self, value):
        if value is None:
            return ""
        eq = " = " if self.eq else " "
        return f"{self.label}{eq}'{value}'"


class FlagProp(Prop):
    def __init__(self, label):
        super().__init__(label, eq=False)
        self.expr = Keywords(self.label)

    def validate(self, _):
        return True

    def render(self, value):
        return self.label if value else ""


class IdentifierProp(Prop):
    def __init__(self, label, **kwargs):
        super().__init__(label, value_expr=pp.Group(FullyQualifiedIdentifier()), **kwargs)

    def validate(self, prop_value):
        return ".".join(prop_value.as_list())

    def render(self, value):
        if value is None:
            return ""
        eq = " = " if self.eq else " "
        return f"{self.label}{eq}{value}"


# FIXME
class IdentifierListProp(Prop):
    def __init__(self, label, **kwargs):
        value_expr = pp.Group(pp.delimited_list(FullyQualifiedIdentifier()))
        # value_expr = pp.Group(FullyQualifiedIdentifier())
        super().__init__(label, value_expr=value_expr, **kwargs)

    def validate(self, prop_values):
        return ".".join(prop_values.as_list())


class StringListProp(Prop):
    def __init__(self, label, **kwargs):
        super().__init__(label, value_expr=ANY(), parens=True, **kwargs)

    def validate(self, prop_value):
        return [[tok.strip(" '") for tok in prop_value]]


class PropSet(Prop):
    def __init__(self, label, props):
        value_expr = LPAREN + ... + RPAREN
        super().__init__(label, value_expr)
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
        label = "TAG"
        super().__init__(label, value_expr=(ANY() + EQUALS() + ANY()), eq=False, parens=True)

    def validate(self, prop_value: list) -> dict:
        pairs = iter(prop_value)
        tags = {}
        for key in pairs:
            tags[key] = next(pairs).strip("'")
        return tags

    def render(self, value: dict) -> str:
        if value is None:
            return ""
        tag_kv_pairs = ", ".join([f"{key} = '{value}'" for key, value in value.items()])
        return f"{self.label} ({tag_kv_pairs})"


class DictProp(Prop):
    """
    HEADERS = ( '<header_1>' = '<value_1>' [ , '<header_2>' = '<value_2>' ... ] )
    """

    def __init__(self, label, **kwargs):
        value_expr = pp.delimited_list(ANY() + EQUALS() + ANY())
        super().__init__(label, value_expr, parens=True, **kwargs)

    def validate(self, prop_value):
        pairs = iter(prop_value)
        values = {}
        for key in pairs:
            values[key.strip("'")] = next(pairs).strip("'")
        return values


class EnumProp(Prop):
    def __init__(self, label, enum_or_list, **kwargs):
        self.enum_type = type(enum_or_list[0]) if isinstance(enum_or_list, list) else enum_or_list
        self.valid_values = set(enum_or_list)
        value_expr = pp.MatchFirst([Keywords(str(val)) for val in self.valid_values]) | ANY
        super().__init__(label, value_expr, **kwargs)

    def validate(self, prop_value):
        parsed = self.enum_type.parse(prop_value.strip("'"))
        if parsed not in self.valid_values:
            raise ValueError(f"Invalid value: {prop_value} must be one of {self.valid_values}")
        return parsed

    def render(self, value):
        if value is None:
            return ""
        eq = " = " if self.eq else " "
        return f"{self.label}{eq}{value}"


class EnumListProp(Prop):
    def __init__(self, label, enum_or_list):
        self.enum_type = type(enum_or_list[0]) if isinstance(enum_or_list, list) else enum_or_list
        self.valid_values = set(enum_or_list)
        enum_values = pp.MatchFirst([Keywords(str(val)) for val in self.valid_values])
        value_expr = enum_values | ANY
        super().__init__(label, value_expr, parens=True)

    def validate(self, prop_values):
        parsed = [self.enum_type.parse(prop_value.strip("'")) for prop_value in prop_values]
        for value in parsed:
            if value not in self.valid_values:
                raise ValueError(f"Invalid value: {value} must be one of {self.valid_values}")
        return [parsed]

    def render(self, values):
        if values is None or len(values) == 0:
            return ""
        eq = " = " if self.eq else " "
        return f"{self.label}{eq}({values})"


class QueryProp(Prop):
    def __init__(self, label):
        value_expr = pp.Word(pp.printables + " \n")
        super().__init__(label, value_expr, eq=False)

    def validate(self, prop_value):
        return prop_value

    def render(self, value):
        if value is None:
            return ""
        return f"{self.label} {value}"


class ExpressionProp(Prop):
    def __init__(self, label):
        value_expr = pp.Empty() + ... + pp.FollowedBy(Keyword("AS"))
        super().__init__(label, value_expr, eq=False)

    def validate(self, prop_value):
        return prop_value.strip()

    def render(self, value):
        if value is None:
            return ""
        return f"{self.label} {value}"


class TimeTravelProp(Prop):
    """
    { AT | BEFORE } ( {
        TIMESTAMP => <timestamp> |
        OFFSET => <time_difference> |
        STATEMENT => <id> |
        STREAM => '<name>'
    } )
    """

    def __init__(self, label):
        value_expr = ANY + ARROW + ANY
        super().__init__(label, value_expr, eq=False, parens=True)

    def validate(self, prop_value):
        key, value = prop_value
        value = value.strip("'")
        return dict([(key, value)])

    def render(self, value):
        if value is None:
            return ""
        return f"{self.label} ({value})"


class AlertConditionProp(Prop):
    def __init__(self):
        label = "IF"
        value_expr = LPAREN + Keyword("EXISTS").suppress() + pp.original_text_for(pp.nested_expr()) + RPAREN
        super().__init__(label, value_expr, eq=False)

    def validate(self, prop_value):
        return prop_value.strip("()").strip()

    def render(self, value):
        if value is None:
            return ""
        return f"IF(EXISTS( {value} ))"


class SessionParametersProp(Prop):
    pass


class ColumnsProp(Prop):
    def __init__(self):
        # super().__init__("columns", value_expr=ANY(), parens=True)
        label = "columns"
        super().__init__(label, eq=False, parens=True)
        arg_type = pp.MatchFirst([Keywords(str(val)) for val in set(DataType)]) | ANY
        self.expr = pp.Group(parens(pp.delimited_list(pp.Group(ANY() + arg_type))))

    def validate(self, prop_values):
        return [prop_values.as_list()]
        # columns = []
        # for col in prop_values:
        #     # tags[key] = next(values).strip("'")
        #     columns.append(
        #         {
        #             "name": key,
        #         }
        #     )
        # return columns
