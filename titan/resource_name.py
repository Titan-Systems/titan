from typing import Union

import pyparsing as pp

from .parse_primitives import FullyQualifiedIdentifier


def attribute_is_resource_name(attribute: str) -> bool:
    return attribute == "name" or attribute == "on" or attribute.endswith("_name") or attribute == "owner"


class ResourceName:
    def __init__(self, name: Union[str, "ResourceName"]) -> None:
        if isinstance(name, ResourceName):
            self._name = name._name
            self._quoted = name._quoted
        elif name.startswith('"') and name.endswith('"'):
            self._name = name[1:-1]
            self._quoted = True
        else:
            self._name = name
            try:
                # If we can parse it, we don't need to quote it
                FullyQualifiedIdentifier.parse_string(name, parse_all=True)
                self._quoted = False
            except pp.ParseException:
                self._quoted = True

    def __repr__(self):
        name = getattr(self, "_name", None)
        quoted = getattr(self, "_quoted", False)
        name = f'"{name}"' if quoted else name
        return f"Resource:{name}"

    def __hash__(self):
        return hash(str(self))

    def __str__(self):
        return f'"{self._name}"' if self._quoted else self._name.upper()

    def __eq__(self, other: Union[str, "ResourceName"]):
        if not isinstance(other, (ResourceName, str)):
            return False
        if isinstance(other, str):
            other = ResourceName(other)
        if self._quoted and other._quoted:
            return self._name == other._name
        elif not self._quoted and not other._quoted:
            return self._name.upper() == other._name.upper()
        else:
            return False

    def upper(self):
        return self

    def startswith(self, prefix: str) -> bool:
        return self._name.startswith(prefix)
