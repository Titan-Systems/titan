from dataclasses import dataclass

from ..enums import ParseableEnum, ResourceType
from ..props import (
    BoolProp,
    EnumProp,
    IdentifierProp,
    Props,
    StringProp,
    TagsProp,
)
from ..resource_name import ResourceName
from ..scope import AccountScope
from .resource import NamedResource, Resource, ResourceSpec
from .role import Role
from .tag import TaggableResource


class ExternalVolumeStorageProvider(ParseableEnum):
    AZURE = "AZURE"
    GCS = "GCS"
    S3 = "S3"
    S3GOV = "S3GOV"


@dataclass(unsafe_hash=True)
class _S3ExternalVolume(ResourceSpec):
    name: ResourceName
    owner: Role = "SYSADMIN"
    volume_type: ExternalVolumeStorageProvider = ExternalVolumeStorageProvider.S3
    storage_locations: list[dict[str, str]] = None
    allow_writes: bool = True
    comment: str = None

    def __post_init__(self):
        super().__post_init__()
        if self.storage_locations is None or len(self.storage_locations) == 0:
            raise ValueError("storage_locations is required")


class S3ExternalVolume(NamedResource, TaggableResource, Resource):
    resource_type = ResourceType.EXTERNAL_VOLUME
    props = Props(
        # volume_type=EnumProp("volume_type", ExternalVolumeType),
        # storage_locations=ListProp("storage_locations", StringProp("location")),
        comment=StringProp("comment"),
        tags=TagsProp(),
    )
    scope = AccountScope()
    spec = _S3ExternalVolume

    def __init__(
        self,
        name: str,
        owner: str = "SYSADMIN",
        storage_locations: list[dict[str, str]] = None,
        allow_writes: bool = True,
        comment: str = None,
        tags: dict[str, str] = None,
        **kwargs,
    ):
        super().__init__(name, **kwargs)
        self._data: _S3ExternalVolume = _S3ExternalVolume(
            name=self._name,
            owner=owner,
            storage_locations=storage_locations,
            allow_writes=allow_writes,
            comment=comment,
        )
        self.set_tags(tags)
