from dataclasses import dataclass

from ..enums import Language, ResourceType
from ..props import (
    EnumProp,
    Props,
    StringListProp,
    StringProp,
)
from ..resource_name import ResourceName
from ..role_ref import RoleRef
from ..scope import SchemaScope
from .resource import NamedResource, Resource, ResourceSpec


@dataclass(unsafe_hash=True)
class _PackagesPolicy(ResourceSpec):
    name: ResourceName
    language: Language = Language.PYTHON
    allowlist: list[str] = None
    blocklist: list[str] = None
    additional_creation_blocklist: list[str] = None
    comment: str = None
    owner: RoleRef = "SYSADMIN"

    def __post_init__(self):
        super().__post_init__()
        if self.language != Language.PYTHON:
            raise ValueError("Language must be PYTHON")


class PackagesPolicy(NamedResource, Resource):
    """
    Description:
        A Packages Policy defines a set of rules for allowed and blocked packages
        that are applied to user-defined functions and stored procedures.

    Snowflake Docs:
        https://docs.snowflake.com/en/sql-reference/sql/create-packages-policy

    Fields:
        name (string, required): The name of the packages policy.
        language (string or Language): The programming language for the packages. Defaults to PYTHON.
        allowlist (list): A list of package specifications that are explicitly allowed.
        blocklist (list): A list of package specifications that are explicitly blocked.
        additional_creation_blocklist (list): A list of package specifications that are blocked during creation.
        comment (string): A comment or description for the packages policy.
        owner (string or Role): The owner role of the packages policy. Defaults to SYSADMIN.

    Python:

        ```python
        packages_policy = PackagesPolicy(
            name="some_packages_policy",
            allowlist=["numpy", "pandas"],
            blocklist=["os", "sys"],
            comment="Policy for data processing packages."
        )
        ```

    Yaml:

        ```yaml
        packages_policy:
          - name: some_packages_policy
            allowlist:
              - numpy
              - pandas
            blocklist:
              - os
              - sys
            comment: Policy for data processing packages.
        ```
    """

    resource_type = ResourceType.PACKAGES_POLICY
    props = Props(
        language=EnumProp("language", [Language.PYTHON], eq=False),
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
