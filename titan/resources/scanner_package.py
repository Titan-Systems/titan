from dataclasses import dataclass

from ..enums import ResourceType
from ..props import (
    Props,
)
from ..resource_name import ResourceName
from ..scope import AccountScope
from .resource import NamedResource, Resource, ResourceSpec


@dataclass(unsafe_hash=True)
class _ScannerPackage(ResourceSpec):
    name: ResourceName
    enabled: bool = True
    schedule: str = "0 0 * * * UTC"

    def __post_init__(self):
        super().__post_init__()
        if self.name == "SECURITY_ESSENTIALS":
            raise ValueError("SECURITY_ESSENTIALS is a system scanner package and cannot be used")


class ScannerPackage(NamedResource, Resource):

    resource_type = ResourceType.SCANNER_PACKAGE
    props = Props()
    scope = AccountScope()
    spec = _ScannerPackage

    def __init__(
        self,
        name: str,
        enabled: bool = True,
        schedule: str = "0 0 * * * UTC",
        **kwargs,
    ):
        super().__init__(name, **kwargs)
        self._data: _ScannerPackage = _ScannerPackage(
            name=self._name,
            enabled=enabled,
            schedule=schedule,
        )
