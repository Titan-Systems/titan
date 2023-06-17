from __future__ import annotations

import re

from enum import Enum
from typing import Union, Optional, Any, Type, TypeVar


Identifier = re.compile(r"[A-Za-z_][A-Za-z0-9_$]*")


class Prop:
    def __init__(self, name, pattern) -> None:
        self.name = name
        # self.prop_type = type
        self.pattern = re.compile(pattern, re.IGNORECASE)

    def search(self, sql: str):
        match = re.search(self.pattern, sql)
        if match:
            return self.normalize(match.group(1))
        return None

    def normalize(self, value: str) -> Any:
        return value

    # def parse(self, value: Union[None, str, Prop]) -> Optional[Prop]:
    #     if value is None:
    #         return None
    #     elif isinstance(value, Prop):
    #         return value
    #     else:
    #         return self._parse(value)

    # def _parse(self, value: Any) -> Optional[Prop]:
    #     raise NotImplementedError


class StringProp(Prop):
    def __init__(self, name) -> None:
        super().__init__(name, rf"\s+{name}\s*=\s*([\w\d_]+|'[^']+')")

    def normalize(self, value: str) -> str:
        return value.strip("'")

    # def _parse(self, value: str) -> StringProp:
    #     if isinstance(value, str):
    #         return value
    #     else:
    #         raise TypeError(f"Invalid {self.name} value: {value}. Must be a string")


class BoolProp(Prop):
    def __init__(self, name) -> None:
        super().__init__(name, rf"\s+{name}\s*=\s*(TRUE|FALSE)")

    def normalize(self, value: str) -> bool:
        return value.upper() == "TRUE"

    # def _parse(self, value: bool) -> BoolProp:
    #     if isinstance(value, bool):
    #         return value
    #     else:
    #         raise TypeError(f"Invalid {self.name} value: {value}. Must be a boolean")


class FlagProp(Prop):
    def __init__(self, name) -> None:
        super().__init__(name, rf"\s+{name}")

    def search(self, sql: str) -> bool:
        match = re.search(self.pattern, sql)
        return match is not None


class IntProp(Prop):
    def __init__(self, name) -> None:
        super().__init__(name, rf"\s+{name}\s*=\s*(\d+)")

    def normalize(self, value: str) -> int:
        return int(value)

    # def _parse(self, value: int) -> IntProp:
    #     if isinstance(value, int):
    #         return value
    #     else:
    #         raise TypeError(f"Invalid {self.name} value: {value}. Must be an integer")


# class EnumProp(Prop):
#     def __init__(self, name, enum) -> None:
#         super().__init__(name, enum, rf"{name}\s*=\s*(TRUE|FALSE)")


class IdentifierProp(Prop):
    def __init__(self, name, pattern=None) -> None:
        super().__init__(name, pattern or rf"\s+{name}\s*=\s*({Identifier.pattern})")

    # def _parse(self, value: str) -> str:
    #     match = Identifier.match(value)
    #     if match:
    #         return value
    #     else:
    #         raise TypeError(f"Invalid {self.name} value: {value}. Must be a valid snowflake identifer")


T_ParseableEnum = TypeVar("T_ParseableEnum", bound="ParsableEnum")


class ParsableEnum(Enum):
    @classmethod
    def parse(cls: Type[T_ParseableEnum], value) -> T_ParseableEnum:
        # if value is None:
        #     return None
        if isinstance(value, cls):
            return value.value
        try:
            x = cls[value.upper().replace("-", "_").replace(" ", "_")]
        except KeyError:
            raise ValueError(f"Invalid {cls.__name__} value: {value}. Must be one of {[e.value for e in cls]}")
        return x.value


class EnumProp(Prop):
    def __init__(self, name, enum_: Type[ParsableEnum]) -> None:
        valid_values = "|".join([e.value for e in enum_])
        super().__init__(name, rf"\s+{name}\s*=\s*({valid_values})")


class TagsProp(Prop):
    """
    [ [ WITH ] TAG ( <tag_name> = '<tag_value>' [ , <tag_name> = '<tag_value>' , ... ] ) ]
    """

    # \s+(?:WITH)?\s+TAG\s+\((\s*\w+\s+=\s+'\w*'\s*,?)+\s*\)

    tag_pattern = rf"(?P<key>{Identifier.pattern})\s*=\s*'(?P<value>[^']*)'"

    def __init__(self) -> None:
        super().__init__("TAGS", r"\s+(?:WITH)?\s+TAG\s*\((?P<tags>.*)\)")

    def normalize(self, value: str) -> Any:
        tag_matches = re.findall(self.tag_pattern, value)
        return {key: value for key, value in tag_matches}

    # def search(self, sql: str):
    #     match = re.search(self.pattern, sql)
    #     if match:
    #         return self.normalize(match.group(1))
    #     return None
