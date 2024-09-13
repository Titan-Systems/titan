import json
import sys
from abc import ABC
from typing import Dict, Optional

import pyparsing as pp

from .builder import tidy_sql
from .enums import DataType
from .parse import (
    ANY,
    ARROW,
    EQUALS,
    FullyQualifiedIdentifier,
    Identifier,
    Keyword,
    Keywords,
    Literals,
    _in_parens,
    _parse_props,
    _parser_has_results_name,
)

__this__ = sys.modules[__name__]


def quote_value(value: str):
    if value is None or value == "":
        return "''"
    return f"$${value}$$"


class Prop(ABC):
    """
    A Prop is a named expression that can be parsed from a SQL string.
    """

    # TODO: find a better home for alt_tokens
    def __init__(self, label, value_expr=ANY(), eq=True, parens=False, alt_tokens=[], consume=[]):
        self.label = label
        self.eq = eq
        self.parens = parens
        self.alt_tokens = set([tok.lower() for tok in alt_tokens])

        if isinstance(consume, str):
            consume = [consume]
        consume_expr = None
        if consume:
            consume_expr = pp.And([pp.Opt(Keyword(tok)) for tok in consume]).suppress()

        label_expr = None
        if self.label:
            label_expr = Keywords(self.label).suppress()

        eq_expr = None
        if self.eq:
            eq_expr = EQUALS()

        if not _parser_has_results_name(value_expr, "prop_value"):
            value_expr = value_expr("prop_value")

        if parens:
            value_expr = _in_parens(value_expr)

        expressions = []
        for expr in [consume_expr, label_expr, consume_expr, eq_expr, value_expr]:
            if expr:
                expressions.append(expr)

        self.parser = pp.And(expressions)

    def __repr__(self):  # pragma: no cover
        return f"{self.__class__.__name__}('{self.label}')"

    def parse(self, sql):
        parsed = self.parser.parse_string(sql)
        prop_value = parsed["prop_value"]
        if isinstance(prop_value, pp.ParseResults):
            prop_value = prop_value.as_list()
        return self.typecheck(prop_value)

    def typecheck(self, prop_value):
        raise NotImplementedError

    def render(self, value):
        raise NotImplementedError


class Props:
    def __init__(self, _name: Optional[str] = None, _start_token: Optional[str] = None, **props: Prop):
        self.props: Dict[str, Prop] = props
        self.name = _name
        self.start_token = Literals(_start_token) if _start_token else None

    def __repr__(self):
        return f"Props(num:{len(self.props)})"

    def __getitem__(self, key: str) -> Prop:
        return self.props[key]

    def to_json(self):
        return json.dumps(self, default=lambda obj: obj.__dict__)

    def render(self, data):
        data = data.copy()
        rendered = []
        for prop_kwarg, prop in self.props.items():
            value = data.pop(prop_kwarg, None)
            if value is None:
                continue
            rendered.append(prop.render(value))
        # if data:
        #     raise RuntimeError(f"Attempted to render unknown properties: {data}")
        return tidy_sql(rendered)


class BoolProp(Prop):
    """
    AUTO_RESUME = { TRUE | FALSE }
    """

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
    """
    AUTO_SUSPEND_SECS = <num>
    """

    def typecheck(self, prop_value):
        try:
            return int(prop_value)
        except ValueError:
            raise ValueError(f"Invalid integer value: {prop_value}")

    def render(self, value):
        if value is None:
            return ""
        return tidy_sql(
            self.label.upper(),
            "=" if self.eq else "",
            value,
        )


class StringProp(Prop):
    """
    COMMENT = '<string_literal>'
    """

    def typecheck(self, prop_value):
        return prop_value

    def render(self, value):
        if value is None:
            return ""
        return tidy_sql(
            self.label.upper(),
            "=" if self.eq else "",
            quote_value(value),
        )


class FlagProp(Prop):
    """
    COPY GRANTS
    """

    def __init__(self, label):
        super().__init__(label, eq=False)
        self.parser = Keywords(self.label)("prop_value")

    def typecheck(self, _):
        return True

    def render(self, value):
        return self.label.upper() if value else ""


class IdentifierProp(Prop):
    """
    WAREHOUSE = <warehouse_name>
    """

    def __init__(self, label, **kwargs):
        super().__init__(label, value_expr=FullyQualifiedIdentifier(), **kwargs)

    def typecheck(self, prop_value):
        return ".".join(prop_value)

    def render(self, value):
        if value is None:
            return ""
        if not isinstance(value, str) and hasattr(value, "name"):
            value = value.name
        return tidy_sql(
            self.label.upper(),
            "=" if self.eq else "",
            value,
        )


class IdentifierListProp(Prop):
    """
    EXTERNAL_ACCESS_INTEGRATIONS = ( <name_of_integration> [ , ... ] )
    """

    def __init__(self, label, **kwargs):
        value_expr = pp.delimited_list(pp.Group(FullyQualifiedIdentifier()))
        super().__init__(label, value_expr=value_expr, **kwargs)

    def typecheck(self, prop_values):
        return [".".join(id_parts) for id_parts in prop_values]

    def render(self, values):
        if values is None:
            return ""
        value_list = ", ".join(map(str, values))
        if self.parens:
            value_list = f"({value_list})"
        return tidy_sql(
            self.label.upper(),
            "=" if self.eq else "",
            value_list,
        )


class StringListProp(Prop):
    """
    PACKAGES = ( '<package_name_and_version>' [ , ... ] )
    """

    def __init__(self, label, **kwargs):
        value_expr = pp.delimited_list(ANY())
        super().__init__(label, value_expr=value_expr, **kwargs)

    def typecheck(self, prop_value):
        return [tok.strip(" ") for tok in prop_value]

    def render(self, values):
        if values is None or len(values) == 0:
            return ""
        value_list = ", ".join([quote_value(v) for v in values])
        return tidy_sql(
            self.label.upper(),
            "=" if self.eq else "",
            f"({value_list})" if self.parens else value_list,
        )


class PropSet(Prop):
    """
    FILE_FORMAT = (
        { FORMAT_NAME = '<file_format_name>' | TYPE = { CSV | JSON | AVRO | ORC | PARQUET | XML | CUSTOM }
    )
    """

    def __init__(self, label, props: Props):
        value_expr = pp.original_text_for(pp.nested_expr())
        super().__init__(label, value_expr)
        self.props: Props = props

    def typecheck(self, prop_value):
        prop_value = prop_value.strip("()")
        return _parse_props(self.props, prop_value)

    def render(self, values):
        if values is None or len(values) == 0:
            return ""
        eq = " = " if self.eq else " "
        value_str = self.props.render(values)
        value_str = f"({value_str})"
        return f"{self.label}{eq}{value_str}"


class PropList(Prop):
    """
    STORAGE_LOCATIONS = (
        (
            NAME = '<storage_location_name>'
            STORAGE_BASE_URL = '<protocol>://<bucket>[/<path>/]'
            ...
      )
      [, (...), ...]
    )
    """

    def __init__(self, label, prop: Prop):
        value_expr = pp.nested_expr(content=pp.delimited_list(pp.original_text_for(pp.nested_expr())))
        super().__init__(label, value_expr)
        self.prop = prop

    def typecheck(self, items: list[list[str]]):
        return [self.prop.parse(item) for item in items[0]]

    def render(self, values):
        if values is None or len(values) == 0:
            return ""
        eq = " = " if self.eq else " "
        return f"{self.label}{eq}({', '.join(map(self.prop.render, values))})"


class StructProp(Prop):
    """
    (
        NAME = '<storage_location_name>'
        STORAGE_BASE_URL = '<protocol>://<bucket>[/<path>/]'
        ...
    )
    """

    def __init__(self, props: Props, **kwargs):
        value_expr = pp.original_text_for(pp.nested_expr())
        super().__init__(label=None, value_expr=value_expr, eq=False, **kwargs)
        self.props = props

    def typecheck(self, payload):
        payload = payload.strip("()")
        return _parse_props(self.props, payload)

    def render(self, value):
        if value is None:
            return ""
        # kv_pairs = " ".join([f"{key} = '{value}'" for key, value in value.items()])
        kv_pairs = self.props.render(value)
        return f"({kv_pairs})"


class TagsProp(Prop):
    """
    [ [ WITH ] TAG ( <tag_name> = '<tag_value>' [ , <tag_name> = '<tag_value>' , ... ] ) ]
    """

    def __init__(self):
        label = "TAG"
        value_expr = pp.delimited_list(ANY() + EQUALS() + ANY())
        super().__init__(label, value_expr=value_expr, eq=False, parens=True, consume="WITH")

    def typecheck(self, prop_value: list) -> dict:
        pairs = iter(prop_value)
        tags = {}
        for key in pairs:
            tags[key] = next(pairs)  # .strip("'")
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
        super().__init__(label, value_expr, **kwargs)

    def typecheck(self, prop_value):
        pairs = iter(prop_value)
        values = {}
        for key in pairs:
            values[key] = next(pairs)
        return values

    def render(self, value: dict) -> str:
        if value is None:
            return ""
        kv_pairs = ", ".join([f"'{key}' = '{value}'" for key, value in value.items()])
        eq = " = " if self.eq else " "
        return f"{self.label}{eq}({kv_pairs})"


class ReturnsProp(Prop):
    """
    RETURNS VARIANT
    RETURNS VARCHAR NOT NULL
    RETURNS NUMBER(38,0)
    RETURNS TABLE (event_date DATE, city VARCHAR, temperature NUMBER)
    """

    def __init__(self, label, **kwargs):
        data_type = pp.MatchFirst([Keywords(val.value) for val in set(DataType)]) | Keyword("TABLE")
        value_expr = pp.delimited_list(data_type + pp.Optional(pp.original_text_for(pp.nested_expr())))
        super().__init__(label, value_expr, **kwargs)

    def typecheck(self, prop_value):
        return "".join(prop_value)
        # if isinstance(prop_value, list):
        #     if len(prop_value) == 1:
        #         return {"data_type": DataType(prop_value[0])}
        #     else:
        #         return {"data_type": DataType(prop_value[0]), "metadata": prop_value[1]}

    def render(self, value):
        if value is None:
            return ""
        eq = " = " if self.eq else " "
        return f"{self.label}{eq}{str(value)}"


class EnumProp(Prop):
    def __init__(self, label, enum_or_list, quoted=False, **kwargs):
        self.enum_type = type(enum_or_list[0]) if isinstance(enum_or_list, list) else enum_or_list
        self.valid_values = set(enum_or_list)
        self.quoted = quoted
        value_expr = pp.MatchFirst([Keywords(val.value) for val in self.valid_values]) | (~Keyword("NULL") + ANY())
        super().__init__(label, value_expr, **kwargs)

    def typecheck(self, prop_value):
        if isinstance(prop_value, list):
            prop_value = prop_value[0]
        prop_value = self.enum_type(prop_value)
        if prop_value not in self.valid_values:
            raise ValueError(f"Invalid value: {prop_value} must be one of {self.valid_values}")
        return prop_value

    def render(self, value):
        if value is None:
            return ""
        eq = " = " if self.eq else " "
        if self.quoted:
            value = f"'{value}'"
        return f"{self.label}{eq}{value}"


class EnumListProp(Prop):
    def __init__(self, label, enum_or_list, **kwargs):
        self.enum_type = type(enum_or_list[0]) if isinstance(enum_or_list, list) else enum_or_list
        self.valid_values = set(enum_or_list)
        enum_values = pp.MatchFirst([Keywords(val.value) for val in self.valid_values])
        value_expr = pp.delimited_list(enum_values | ANY())
        super().__init__(label, value_expr, **kwargs)

    def typecheck(self, prop_values):
        prop_values = [self.enum_type(val) for val in prop_values]
        for value in prop_values:
            if value not in self.valid_values:
                raise ValueError(f"Invalid value: {value} must be one of {self.valid_values}")
        return prop_values

    def render(self, values):
        if values is None or len(values) == 0:
            return ""
        eq = " = " if self.eq else " "
        value_list = ", ".join([str(val) for val in values])
        return f"{self.label}{eq}{value_list}"


class EnumFlagProp(Prop):
    def __init__(self, enum_or_list, **kwargs):
        self.enum_type = type(enum_or_list[0]) if isinstance(enum_or_list, list) else enum_or_list
        self.valid_values = set(enum_or_list)
        value_expr = pp.MatchFirst([Keywords(val.value) for val in self.valid_values])
        super().__init__(label=None, value_expr=value_expr, eq=False, **kwargs)

    def typecheck(self, prop_value):
        prop_value = self.enum_type(prop_value)
        if prop_value not in self.valid_values:
            raise ValueError(f"Invalid value: {prop_value} must be one of {self.valid_values}")
        return prop_value

    def render(self, value):
        if value is None:
            return ""
        return value


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
        value_expr = ANY() + ARROW + ANY()
        super().__init__(label, value_expr, eq=False, parens=True)

    def typecheck(self, prop_value):
        key, value = prop_value
        return {key: value}

    def render(self, values):
        if values is None:
            return ""

        key, value = values.popitem()
        if key.upper() == "STREAM":
            value = f"'{value}'"
        time_point = f"{key} => {value}"
        return f"{self.label} ({time_point})"


class AlertConditionProp(Prop):
    def __init__(self):
        label = "IF"
        value_expr = Keyword("EXISTS").suppress() + pp.original_text_for(pp.nested_expr())("prop_value")
        super().__init__(label, value_expr, eq=False, parens=True)

    def typecheck(self, prop_value):
        return prop_value.strip("()").strip()

    def render(self, value):
        if value is None:
            return ""
        return f"IF(EXISTS( {value} ))"


class SessionParametersProp(Prop):
    pass


class ArgsProp(Prop):
    def __init__(self):
        value_expr = pp.original_text_for(pp.nested_expr())
        super().__init__(label=None, value_expr=value_expr, eq=False)

    def typecheck(self, prop_values):
        arg_parser = pp.delimited_list(
            pp.Group(
                (Identifier | pp.dbl_quoted_string)("name")
                + ANY("data_type")
                + pp.Opt(_in_parens(ANY()))("data_type_size")
            )
        )
        prop_values = prop_values.strip("()".strip())
        if prop_values == "":
            return []
        parsed = arg_parser.parse_string(prop_values)
        args = []
        for arg_data in parsed:
            arg = arg_data.as_dict()
            arg["name"] = arg["name"].strip('"')
            arg["data_type"] = DataType(arg["data_type"])
            args.append(arg)
        return args

    def render(self, value):
        if value is None or len(value) == 0:
            return "()"
        args = []
        for arg in value:
            default = f" DEFAULT {arg['default']}" if arg.get("default") else ""
            args.append(f"{arg['name']} {str(arg['data_type'])}{default}")
        return f"({', '.join(args)})"


class ColumnNamesProp(Prop):
    def __init__(self):
        value_expr = pp.original_text_for(pp.nested_expr())
        super().__init__(label=None, value_expr=value_expr, eq=False)

    def typecheck(self, prop_values):
        prop_values = prop_values.strip("()")
        column_name_parser = pp.delimited_list(
            pp.Group(
                (Identifier() | pp.dbl_quoted_string)("name")
                + pp.Opt(Keyword("COMMENT") + pp.sgl_quoted_string("comment"))
            )
        )
        parsed = column_name_parser.parse_string(prop_values)
        columns = []
        for column_data in parsed:
            column = column_data.as_dict()
            column["name"] = column["name"].strip('"')
            if "comment" in column:
                column["comment"] = column["comment"].strip("'")
            columns.append(column)
        return columns

    def render(self, values):
        if values is None or len(values) == 0:
            return "()"
        columns = []
        for column in values:
            name = column["name"]
            comment = f" COMMENT '{column['comment']}'" if "comment" in column else ""
            column_str = f"{name}{comment}"
            columns.append(column_str)
        return f"({', '.join(columns)})"


class SchemaProp(Prop):
    def __init__(self):
        super().__init__(label=None, value_expr=pp.NoMatch())

    def typecheck(self, prop_values):
        pass

    def render(self, values):
        if values is None or len(values) == 0:
            return "()"
        columns = []
        for column in values:
            name = column["name"]
            data_type = str(column["data_type"])
            not_null = " NOT NULL" if column["not_null"] else ""

            if isinstance(column["default"], str):
                default = f" DEFAULT '{column['default']}'"
            elif column["default"] is not None:
                default = f" DEFAULT {column['default']}"
            else:
                default = ""

            comment = f" COMMENT '{column['comment']}'" if "comment" in column and column["comment"] else ""
            column_str = f"{name} {data_type}{not_null}{default}{comment}"
            columns.append(column_str)
        return f"({', '.join(columns)})"
