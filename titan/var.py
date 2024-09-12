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

    def __eq__(self, other: str):
        raise NotImplementedError("VarString does not support equality checks")


def __getattr__(name):
    # This function will be called when an attribute is not found in the module
    # You can implement your logic here to return dynamic properties
    return VarString("{{var." + name + "}}")


def string_contains_var(string: str):
    return "{{" in string and "}}" in string
