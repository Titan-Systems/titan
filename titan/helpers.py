from __future__ import annotations

from enum import Enum


class ParsableEnum(Enum):
    @classmethod
    def parse(cls, value) -> ParsableEnum:
        if isinstance(value, cls):
            return value.value
        try:
            x = cls[value.upper().replace("-", "_")]
        except KeyError:
            raise ValueError(f"Invalid {cls.__name__} value: {value}. Must be one of {[e.value for e in cls]}")
        return x.value
