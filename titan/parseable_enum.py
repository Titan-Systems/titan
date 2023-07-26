from enum import Enum
from typing import TypeVar, Type, Union

T_ParseableEnum = TypeVar("T_ParseableEnum", bound="ParseableEnum")


class ParseableEnum(Enum):
    @classmethod
    def parse(cls: Type[T_ParseableEnum], value: Union[str, T_ParseableEnum]) -> T_ParseableEnum:
        if cls == ParseableEnum:
            raise TypeError(f"Only children of '{cls.__name__}' may be instantiated")

        if isinstance(value, cls):
            return value
        elif isinstance(value, str):
            try:
                parsed = cls[value.upper().replace("-", "_").replace(" ", "_")]
            except KeyError:
                raise ValueError(
                    f"Invalid {cls.__name__} value: {value}. Must be one of {[e.value for e in cls]}"
                )
            return parsed
        else:
            raise ValueError(f"Invalid {cls.__name__} value: {value}")

    def __str__(self) -> str:
        return self.value
