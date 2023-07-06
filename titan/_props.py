from __future__ import annotations

import re

from enum import Enum
from typing import Union, List, Optional, Any, Type, TypeVar

Identifier = re.compile(r"[A-Za-z_][A-Za-z0-9_$]*")
QuotedString = r"'[^']*'"


class Prop:
    def __init__(self, name, pattern) -> None:
        self.name = name
        self.pattern = re.compile(r"(?:^|\W)+" + pattern, re.IGNORECASE | re.MULTILINE)

    def scanner(self, prop_kwarg: str):
        return (self.pattern, lambda _, t: (prop_kwarg, self.normalize(t)))

    def search(self, sql: str):
        match = re.search(self.pattern, sql)
        if match:
            return self.normalize(match.group(1))
        return None

    def normalize(self, value: str) -> Any:
        return value

    def render(self, value: Optional[Any]) -> str:
        if value is None:
            return ""
        return f"{self.name} = {value}"


class StringProp(Prop):
    def __init__(self, name, valid_values: List[Union[str, ParseableEnum]] = [], alt_tokens: List[str] = []) -> None:
        if valid_values:
            string_types = "|".join([f"'{str(val)}'" for val in valid_values])
        else:
            # r"[\w\d_]+"
            string_types = "|".join([Identifier.pattern, QuotedString] + alt_tokens)
        pattern = rf"{name}\s*=\s*({string_types})"
        super().__init__(name, pattern)
        self.valid_values = valid_values

    def normalize(self, value: str) -> str:
        return value.strip("'")

    def render(self, value: Optional[str]) -> str:
        if value is None:
            return ""
        return f"{self.name} = '{value}'"


# class StringListProp(Prop):
#     def __init__(self, name) -> None:
#         super().__init__(name, rf"{name}\s*=\s*\((?P<strings>.*)\)")

#     def normalize(self, value: str) -> Any:
#         matches = re.findall(QuotedString, value)
#         return [match.strip("'") for match in matches]

#     def render(self, values: Optional[List[Any]]) -> str:
#         if values:
#             strings = ", ".join([f"'{item}'" for item in values])
#             return f"{self.name} = ({strings})"
#         else:
#             return ""


# class BoolProp(Prop):
#     def __init__(self, name) -> None:
#         super().__init__(name, rf"{name}\s*=\s*(TRUE|FALSE)")

#     def normalize(self, value: str) -> bool:
#         return value.upper() == "TRUE"

#     def render(self, value: Optional[bool]) -> str:
#         if value is None:
#             return ""
#         return f"{self.name} = {str(value).upper()}"


# class FlagProp(Prop):
#     def __init__(self, name) -> None:
#         super().__init__(name, rf"{name}")

#     def search(self, sql: str) -> bool:
#         match = re.search(self.pattern, sql)
#         return match is not None

#     def render(self, value: Optional[bool]) -> str:
#         if value is None:
#             return ""
#         return self.name.upper() if value else ""


# class IntProp(Prop):
#     def __init__(self, name) -> None:
#         super().__init__(name, rf"{name}\s*=\s*(\d+)")

#     def normalize(self, value: str) -> int:
#         return int(value)


# class IdentifierProp(Prop):
#     def __init__(self, name, pattern=None) -> None:
#         super().__init__(name, pattern or rf"{name}\s*=\s*({Identifier.pattern})")

#     def render(self, value: Optional["Resource"]) -> str:  # type: ignore
#         if value is None:
#             return ""
#         return f"{self.name} = {value.fully_qualified_name}"


# class IdentifierListProp(Prop):
#     def __init__(self, name) -> None:
#         super().__init__(name, rf"{name}\s*=\s*\((?P<identifiers>.*)\)")

#     def normalize(self, value: str) -> Any:
#         identifier_matches = re.findall(Identifier.pattern, value)
#         return [match for match in identifier_matches]

#     def render(self, value: Optional[List[Any]]) -> str:
#         if value:
#             # tag_kv_pairs = ", ".join([f"{key} = '{value}'" for key, value in value.items()])
#             # TODO: wtf is this
#             identifiers = ", ".join([str(id) for id in value])
#             return f"{self.name} = ({identifiers})"
#         else:
#             return ""


# T_ParseableEnum = TypeVar("T_ParseableEnum", bound="ParseableEnum")


# class ParseableEnum(Enum):
#     @classmethod
#     def parse(cls: Type[T_ParseableEnum], value: Union[str, T_ParseableEnum]) -> T_ParseableEnum:
#         if cls == ParseableEnum:
#             raise TypeError(f"Only children of '{cls.__name__}' may be instantiated")

#         if isinstance(value, cls):
#             return value
#         elif isinstance(value, str):
#             try:
#                 parsed = cls[value.upper().replace("-", "_").replace(" ", "_")]
#             except KeyError:
#                 raise ValueError(f"Invalid {cls.__name__} value: {value}. Must be one of {[e.value for e in cls]}")
#             return parsed
#         else:
#             raise ValueError(f"Invalid {cls.__name__} value: {value}")

#     def __str__(self) -> str:
#         return self.value


# class EnumProp(Prop):
#     def __init__(self, name, enum_: Union[Type[ParseableEnum], List[ParseableEnum]]) -> None:
#         valid_values = set(enum_)
#         value_pattern = "|".join([e.value for e in valid_values] + [f"'{e.value}'" for e in valid_values])
#         super().__init__(name, rf"{name}\s*=\s*({value_pattern})")
#         self.enum = type(enum_[0]) if isinstance(enum_, list) else enum_

#     def normalize(self, value: str) -> ParseableEnum:
#         return self.enum.parse(value.strip("'"))

#     def render(self, value: Optional[ParseableEnum]) -> str:
#         if value is None:
#             return ""
#         return f"{self.name} = {value.value}"


# class PropList(Prop):
#     def __init__(self, name, expected_props) -> None:
#         super().__init__(name, rf"{name}\s*=\s*\((.*)\)")
#         self.expected_props = expected_props

#     def normalize(self, value: str) -> Any:
#         normalized = {}
#         for name, prop in self.expected_props.items():
#             match = prop.search(value)
#             if match:
#                 normalized[name.lower()] = match
#         return normalized if normalized else None

#     def render(self, values: Optional[dict]) -> str:
#         if values is None or len(values) == 0:
#             return ""
#         kv_pairs = []
#         for name, prop in self.expected_props.items():
#             if name.lower() in values:
#                 kv_pairs.append(prop.render(values[name.lower()]))

#         return f"{self.name} = ({', '.join(kv_pairs)})"


# class TagsProp(Prop):
#     """
#     [ [ WITH ] TAG ( <tag_name> = '<tag_value>' [ , <tag_name> = '<tag_value>' , ... ] ) ]
#     """

#     tag_pattern = rf"(?P<key>{Identifier.pattern})\s*=\s*'(?P<value>[^']*)'"

#     def __init__(self) -> None:
#         super().__init__("TAGS", r"(?:WITH)?\s+TAG\s*\((?P<tags>.*)\)")

#     def normalize(self, value: str) -> Any:
#         tag_matches = re.findall(self.tag_pattern, value)
#         return {key: value for key, value in tag_matches}

#     def render(self, value: Any) -> str:
#         if value:
#             tag_kv_pairs = ", ".join([f"{key} = '{value}'" for key, value in value.items()])
#             return f"WITH TAG ({tag_kv_pairs})"
#         else:
#             return ""


# class QueryProp(Prop):
#     def __init__(self, name) -> None:
#         super().__init__(name, rf"{name}\s+([^;]*)")

#     # def normalize(self, value: str) -> str:
#     #     return value.strip("'").upper()

#     def render(self, value: Optional[str]) -> str:
#         if value is None:
#             return ""
#         return f"{self.name} {value}"
