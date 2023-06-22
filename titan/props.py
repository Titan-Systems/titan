from __future__ import annotations

import re

from enum import Enum
from typing import Union, Optional, Any, Type, TypeVar


Identifier = re.compile(r"[A-Za-z_][A-Za-z0-9_$]*")


class Prop:
    def __init__(self, name, pattern) -> None:
        self.name = name
        self.pattern = re.compile(pattern, re.IGNORECASE)

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
    def __init__(self, name) -> None:
        super().__init__(name, rf"\s+{name}\s*=\s*([\w\d_]+|'[^']+')")

    def normalize(self, value: str) -> str:
        return value.strip("'")

    def render(self, value: Optional[str]) -> str:
        if value is None:
            return ""
        return f"{self.name} = '{value}'"


class BoolProp(Prop):
    def __init__(self, name) -> None:
        super().__init__(name, rf"\s+{name}\s*=\s*(TRUE|FALSE)")

    def normalize(self, value: str) -> bool:
        return value.upper() == "TRUE"

    def render(self, value: Optional[bool]) -> str:
        if value is None:
            return ""
        return f"{self.name} = {str(value).upper()}"


class FlagProp(Prop):
    def __init__(self, name) -> None:
        super().__init__(name, rf"\s+{name}")

    def search(self, sql: str) -> bool:
        match = re.search(self.pattern, sql)
        return match is not None

    def render(self, value: Optional[bool]) -> str:
        if value is None:
            return ""
        return self.name.upper() if value else ""


class IntProp(Prop):
    def __init__(self, name) -> None:
        super().__init__(name, rf"\s+{name}\s*=\s*(\d+)")

    def normalize(self, value: str) -> int:
        return int(value)


class IdentifierProp(Prop):
    def __init__(self, name, pattern=None) -> None:
        super().__init__(name, pattern or rf"\s+{name}\s*=\s*({Identifier.pattern})")

    def render(self, value: Optional["Resource"]) -> str:  # type: ignore
        if value is None:
            return ""
        return f"{self.name} = {value.fully_qualified_name}"


class IdentifierListProp(Prop):
    def __init__(self, name) -> None:
        super().__init__(name, rf"\s+{name}\s*=\s*\((?P<identifiers>.*)\)")

    def normalize(self, value: str) -> Any:
        identifier_matches = re.findall(Identifier.pattern, value)
        return [match for match in identifier_matches]

    def render(self, value: Any) -> str:
        if value:
            tag_kv_pairs = ", ".join([f"{key} = '{value}'" for key, value in value.items()])
            return f"WITH TAG ({tag_kv_pairs})"
        else:
            return ""


T_ParseableEnum = TypeVar("T_ParseableEnum", bound="ParsableEnum")


class ParsableEnum(Enum):
    @classmethod
    def parse(cls: Type[T_ParseableEnum], value) -> T_ParseableEnum:
        if isinstance(value, cls):
            return value.value
        try:
            x = cls[value.upper().replace("-", "_").replace(" ", "_")]
        except KeyError:
            raise ValueError(f"Invalid {cls.__name__} value: {value}. Must be one of {[e.value for e in cls]}")
        return x.value

    def __str__(self) -> str:
        return self.value


class EnumProp(Prop):
    def __init__(self, name, enum_: Type[ParsableEnum]) -> None:
        valid_values = [e.value for e in enum_]
        valid_values += [f"'{e.value}'" for e in enum_]
        super().__init__(name, rf"\s+{name}\s*=\s*({'|'.join(valid_values)})")

    def normalize(self, value: str) -> str:
        return value.strip("'")

    def render(self, value: Optional[ParsableEnum]) -> str:
        if value is None:
            return ""
        return f"{self.name} = '{value.value}'"


class TagsProp(Prop):
    """
    [ [ WITH ] TAG ( <tag_name> = '<tag_value>' [ , <tag_name> = '<tag_value>' , ... ] ) ]
    """

    tag_pattern = rf"(?P<key>{Identifier.pattern})\s*=\s*'(?P<value>[^']*)'"

    def __init__(self) -> None:
        super().__init__("TAGS", r"\s+(?:WITH)?\s+TAG\s*\((?P<tags>.*)\)")

    def normalize(self, value: str) -> Any:
        tag_matches = re.findall(self.tag_pattern, value)
        return {key: value for key, value in tag_matches}

    def render(self, value: Any) -> str:
        if value:
            tag_kv_pairs = ", ".join([f"{key} = '{value}'" for key, value in value.items()])
            return f"WITH TAG ({tag_kv_pairs})"
        else:
            return ""
