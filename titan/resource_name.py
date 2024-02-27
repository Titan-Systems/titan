from typing import Union

import pyparsing as pp

from .parse import FullyQualifiedIdentifier


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

    def __hash__(self):
        return hash(str(self))

    def __str__(self):
        return f'"{self._name}"' if self._quoted else self._name

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
