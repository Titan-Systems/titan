from abc import ABC
from typing import Dict

import pyparsing as pp

from .builder import tidy_sql
from .enums import DataType
from .parse import (
    _parser_has_results_name,
    _parse_props,
    Keyword,
    Keywords,
    Literals,
    list_expr,
    parens,
    FullyQualifiedIdentifier,
    LPAREN,
    RPAREN,
    EQUALS,
    ARROW,
    ANY,
)


class Prop(ABC):
    """
    A Prop is a named expression that can be parsed from a SQL string.
    """

    # TODO: find a better home for alt_tokens
    def __init__(self, label, value_expr=ANY(), eq=True, parens=False, alt_tokens=[], consume=[]):
        self.label = label
        self.eq = eq
        self.alt_tokens = set([tok.lower() for tok in alt_tokens])

        eq_expr = EQUALS() if eq else pp.Empty()

        if isinstance(consume, str):
            consume = [consume]
        consume_expr = pp.Empty()
        if consume:
            consume_expr = pp.And([pp.Opt(Keyword(tok)) for tok in consume])

        if parens:
            value_expr = list_expr(value_expr)

        if not _parser_has_results_name(value_expr, "prop_value"):
            value_expr = value_expr("prop_value")

        self.parser = Keywords(self.label).suppress() + consume_expr.suppress() + eq_expr.suppress() + value_expr

    def __repr__(self):
        return f"{self.__class__.__name__}('{self.label}')"

    def parse(self, sql):
        try:
            prop_value = self.parser.parse_string(sql)["prop_value"]
            if isinstance(prop_value, pp.ParseResults):
                prop_value = prop_value.as_list()
            return self.typecheck(prop_value)
        except pp.ParseException as err:
            print(err)
            return None

    def typecheck(self, prop_value):
        raise NotImplementedError

    def render(self, value):
        raise NotImplementedError


class Props:
    def __init__(self, _name: str = None, _start_token: str = None, **props: Dict[str, Prop]):
        self.props: Dict[str, Prop] = props
        self.name = _name
        self.start_token = Literals(_start_token) if _start_token else None

    def __getitem__(self, key: str) -> Prop:
        return self.props[key]

    def render(self, resource):
        resource_fields = resource.model_json_schema()["properties"]
        data = resource.model_dump(exclude_none=True)

        rendered = []
        for prop_kwarg, prop in self.props.items():
            value = data.get(prop_kwarg)
            if value is None:
                continue
            field_has_default = "default" in resource_fields[prop_kwarg]
            value_is_default = value == resource_fields[prop_kwarg].get("default")
            if not (field_has_default and value_is_default):
                rendered.append(prop.render(value))
        return tidy_sql(rendered)


class BoolProp(Prop):
    def typecheck(self, prop_value):
        if prop_value.lower() not in ["true", "false"]:
            raise ValueError(f"Invalid boolean value: {prop_value}")
        return prop_value.lower() == "true"

    def render(self, value):
        if value is None:
            return ""
        return tidy_sql(
            self.label.upper(),
            "=" if self.eq else "",
            str(value).upper(),
        )


class IntProp(Prop):
    def typecheck(self, prop_value):
        try:
            return int(prop_value)
        except ValueError:
            raise ValueError(f"Invalid integer value: {prop_value}")

    def render(self, value):
        if value is None:
            return ""
        if self.label == "STATEMENT_QUEUED_TIMEOUT_IN_SECONDS":
            print("ok")
        return tidy_sql(
            self.label.upper(),
            "=" if self.eq else "",
            value,
        )


class StringProp(Prop):
    def typecheck(self, prop_value):
        return prop_value.strip("'")

    def render(self, value):
        if value is None:
            return ""
        return tidy_sql(
            self.label.upper(),
            "=" if self.eq else "",
            f"'{value}'",
        )


class FlagProp(Prop):
    def __init__(self, label):
        super().__init__(label, eq=False)
        self.parser = Keywords(self.label)("prop_value")

    def typecheck(self, _):
        return True

    def render(self, value):
        return self.label.upper() if value else ""


class IdentifierProp(Prop):
    def __init__(self, label, **kwargs):
        super().__init__(label, value_expr=FullyQualifiedIdentifier(), **kwargs)

    def typecheck(self, prop_value):
        return ".".join(prop_value)

    def render(self, value):
        if value is None:
            return ""
        return tidy_sql(
            self.label.upper(),
            "=" if self.eq else "",
            value,
        )


# FIXME
class IdentifierListProp(Prop):
    def __init__(self, label, **kwargs):
        value_expr = pp.DelimitedList(pp.Group(FullyQualifiedIdentifier()))
        super().__init__(label, value_expr=value_expr, **kwargs)

    def typecheck(self, prop_values):
        return [".".join(id_parts) for id_parts in prop_values]

    def render(self, values):
        raise NotImplementedError


class StringListProp(Prop):
    def __init__(self, label, **kwargs):
        super().__init__(label, value_expr=ANY(), parens=True, **kwargs)

    def typecheck(self, prop_value):
        return [tok.strip(" '") for tok in prop_value]

    def render(self, values):
        if values is None:
            return ""
        value_list = ", ".join(values)
        return tidy_sql(
            self.label.upper(),
            "=" if self.eq else "",
            f"({value_list})",
        )


class PropSet(Prop):
    def __init__(self, label, props: Props):
        value_expr = pp.original_text_for(pp.nested_expr())
        super().__init__(label, value_expr)
        self.props: Props = props

    def typecheck(self, prop_value):
        prop_value = prop_value.strip("()")
        return _parse_props(self.props, prop_value)

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

    def typecheck(self, prop_value: list) -> dict:
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
        value_expr = pp.DelimitedList(ANY() + EQUALS() + ANY())
        super().__init__(label, value_expr, parens=True, **kwargs)

    def typecheck(self, prop_value):
        pairs = iter(prop_value)
        values = {}
        for key in pairs:
            values[key.strip("'")] = next(pairs).strip("'")
        return values


class EnumProp(Prop):
    def __init__(self, label, enum_or_list, **kwargs):
        self.enum_type = type(enum_or_list[0]) if isinstance(enum_or_list, list) else enum_or_list
        self.valid_values = set(enum_or_list)
        value_expr = pp.MatchFirst([Keywords(val.value) for val in self.valid_values]) | ANY
        super().__init__(label, value_expr, **kwargs)

    def typecheck(self, prop_value):
        prop_value = prop_value.strip("'")
        prop_value = self.enum_type(prop_value)
        if prop_value not in self.valid_values:
            raise ValueError(f"Invalid value: {prop_value} must be one of {self.valid_values}")
        return prop_value

    def render(self, value):
        if value is None:
            return ""
        eq = " = " if self.eq else " "
        return f"{self.label}{eq}{value}"


class EnumListProp(Prop):
    def __init__(self, label, enum_or_list):
        self.enum_type = type(enum_or_list[0]) if isinstance(enum_or_list, list) else enum_or_list
        self.valid_values = set(enum_or_list)
        enum_values = pp.MatchFirst([Keywords(val.value) for val in self.valid_values])
        value_expr = enum_values | ANY
        super().__init__(label, value_expr, parens=True)

    def typecheck(self, prop_values):
        prop_values = [val.strip("'") for val in prop_values]
        prop_values = [self.enum_type(val) for val in prop_values]
        for value in prop_values:
            if value not in self.valid_values:
                raise ValueError(f"Invalid value: {value} must be one of {self.valid_values}")
        # return [parsed]
        return prop_values

    def render(self, values):
        if values is None or len(values) == 0:
            return ""
        eq = " = " if self.eq else " "
        return f"{self.label}{eq}({values})"


class QueryProp(Prop):
    def __init__(self, label):
        value_expr = pp.Word(pp.printables + " \n")
        super().__init__(label, value_expr, eq=False)

    def typecheck(self, prop_value):
        return prop_value

    def render(self, value):
        if value is None:
            return ""
        return f"{self.label} {value}"


class ExpressionProp(Prop):
    def __init__(self, label):
        value_expr = pp.Empty() + pp.SkipTo(Keyword("AS"))("prop_value")
        super().__init__(label, value_expr, eq=False)

    def typecheck(self, prop_value):
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

    def typecheck(self, prop_value):
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
        value_expr = (
            LPAREN + Keyword("EXISTS").suppress() + pp.original_text_for(pp.nested_expr())("prop_value") + RPAREN
        )
        super().__init__(label, value_expr, eq=False)

    def typecheck(self, prop_value):
        return prop_value.strip("()").strip()

    def render(self, value):
        if value is None:
            return ""
        return f"IF(EXISTS( {value} ))"


class SessionParametersProp(Prop):
    pass


class ColumnsProp(Prop):
    def __init__(self):
        super().__init__(label="columns", eq=False, parens=True)
        arg_type = pp.MatchFirst([Keywords(str(val)) for val in set(DataType)]) | ANY
        self.parser = parens(pp.DelimitedList(pp.Group(ANY() + arg_type)))("prop_value")

    def typecheck(self, prop_values):
        columns = []
        for col_name, col_type in prop_values:
            columns.append(
                {
                    "name": col_name,
                    "type": col_type,
                }
            )
        return columns


class ColumnsSchemaProp(Prop):
    def __init__(self):
        super().__init__(label="columns", eq=False, parens=True)
        comment = StringProp("comment", eq=False).parser
        self.parser = parens(pp.DelimitedList(ANY() + pp.Opt(comment)))("prop_value")

    def typecheck(self, prop_values):
        columns = []
        for col_name, comment in prop_values:
            columns.append(
                {
                    "name": col_name,
                    "comment": comment,
                }
            )
        return columns
