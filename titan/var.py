from typing import Any

import jinja2.exceptions
from jinja2 import Environment, StrictUndefined

from .exceptions import MissingVarException

GLOBAL_JINJA_ENV = Environment(undefined=StrictUndefined)


class VarString:
    def __init__(self, string: str):
        self.string = string

    def to_string(self, vars: dict):
        try:
            return GLOBAL_JINJA_ENV.from_string(self.string).render(var=vars)
        except jinja2.exceptions.UndefinedError:
            raise MissingVarException(f"Missing var: {self.string}")

    def __eq__(self, other: Any):
        return False

    def __repr__(self):
        return f"VarString({self.string})"


class VarStub(dict):
    def __missing__(self, key) -> str:
        # Return the string "{{ var.key }}" if the key is not found
        return f"{{{{ var.{key} }}}}"


def __getattr__(name) -> VarString:
    # This function will be called when an attribute is not found in the module
    # You can implement your logic here to return dynamic properties
    return VarString("{{var." + name + "}}")


def string_contains_var(string: str) -> bool:
    return "{{" in string and "}}" in string


def process_for_each(resource_value: str, each_value: str) -> str:
    vars = VarStub()
    return GLOBAL_JINJA_ENV.from_string(resource_value).render(var=vars, each={"value": each_value})
