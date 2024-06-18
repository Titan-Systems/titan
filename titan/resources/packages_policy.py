from dataclasses import dataclass

from .resource import Resource, ResourceSpec, ResourceNameTrait
from .role import Role
from ..enums import ResourceType, Language
from ..scope import SchemaScope
from ..resource_name import ResourceName
from ..props import (
    EnumProp,
    Props,
    StringListProp,
    StringProp,
)


@dataclass(unsafe_hash=True)
class _PackagesPolicy(ResourceSpec):
    name: ResourceName
    language: Language = Language.PYTHON
    allowlist: list[str] = None
    blocklist: list[str] = None
    additional_creation_blocklist: list[str] = None
    comment: str = None
    owner: Role = "SYSADMIN"

    def __post_init__(self):
        super().__post_init__()
        if self.language != Language.PYTHON:
            raise ValueError("Language must be PYTHON")


class PackagesPolicy(ResourceNameTrait, Resource):
    """
    A Packages Policy defines a set of rules for allowed and blocked packages.

    CREATE [ OR REPLACE ] PACKAGES POLICY [ IF NOT EXISTS ] <name>
      LANGUAGE PYTHON
      [ ALLOWLIST = ( [ '<packageSpec>' ] [ , '<packageSpec>' ... ] ) ]
      [ BLOCKLIST = ( [ '<packageSpec>' ] [ , '<packageSpec>' ... ] ) ]
      [ ADDITIONAL_CREATION_BLOCKLIST = ( [ '<packageSpec>' ] [ , '<packageSpec>' ... ] ) ]
      [ COMMENT = '<string_literal>' ]
    """

    resource_type = ResourceType.PACKAGES_POLICY
    props = Props(
        language=EnumProp("language", Language, eq=False),
        allowlist=StringListProp("allowlist", parens=True),
        blocklist=StringListProp("blocklist", parens=True),
        additional_creation_blocklist=StringListProp("additional_creation_blocklist", parens=True),
        comment=StringProp("comment"),
    )
    scope = SchemaScope()
    spec = _PackagesPolicy

    def __init__(
        self,
        name: str,
        language: Language = Language.PYTHON,
        allowlist: list[str] = None,
        blocklist: list[str] = None,
        additional_creation_blocklist: list[str] = None,
        comment: str = None,
        owner: str = "SYSADMIN",
        **kwargs,
    ):
        super().__init__(name, **kwargs)
        self._data: _PackagesPolicy = _PackagesPolicy(
            name=self._name,
            language=language,
            allowlist=allowlist,
            blocklist=blocklist,
            additional_creation_blocklist=additional_creation_blocklist,
            comment=comment,
            owner=owner,
        )
