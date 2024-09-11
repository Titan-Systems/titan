from jinja2 import Template


class VarString:
    def __init__(self, string: str):
        self.string = string

    def to_string(self, vars: dict):
        return Template(self.string).render(var=vars)

    def __eq__(self, other: str):
        raise NotImplementedError("VarString does not support equality checks")


def __getattr__(name):
    # This function will be called when an attribute is not found in the module
    # You can implement your logic here to return dynamic properties
    return VarString("{{var." + name + "}}")


def string_contains_var(string: str):
    return "{{" in string and "}}" in string
