class PropGenerator:
    __slots__ = ("name", "value")

    def __init__(self, name, value):
        self.name = name
        self.value = value

    def value_literal(self):
        if isinstance(self.value, str):
            return f"'{self.value}'"
        elif isinstance(self.value, bool):
            return "TRUE" if self.value else "FALSE"
        elif isinstance(self.value, int):
            return f"{self.value}"
        else:
            raise NotImplementedError


class EqualsProp(PropGenerator):
    def __format__(self, _):
        if self.value is None:
            return ""
        return f"{self.name} = {self.value_literal()}"


class FlagProp(PropGenerator):
    def __format__(self, _):
        if self.value is None:
            return ""
        return self.name if self.value else ""
