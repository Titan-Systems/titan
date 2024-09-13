from dataclasses import dataclass, field

from ..enums import ParseableEnum, ResourceType, EncryptionType
from ..props import (
    StructProp,
    EnumProp,
    PropSet,
    Props,
    StringProp,
    TagsProp,
    PropList,
)
from ..resource_name import ResourceName
from ..scope import AccountScope, AnonymousScope
from .resource import NamedResource, Resource, ResourceSpec
from .role import Role
from .tag import TaggableResource


class ExternalVolumeStorageProvider(ParseableEnum):
    AZURE = "AZURE"
    GCS = "GCS"
    S3 = "S3"
    S3GOV = "S3GOV"


@dataclass(unsafe_hash=True)
class _ExternalVolumeStorageLocation(ResourceSpec):
    name: str
    storage_provider: ExternalVolumeStorageProvider
    storage_base_url: str
    encryption: dict = None
    storage_aws_role_arn: str = None
    storage_aws_external_id: str = None
    # storage_allowed_locations: list[str] = field(default=None, metadata={"known_after_apply": True})
    # storage_aws_iam_user_arn: str = field(default=None, metadata={"known_after_apply": True})

    def __post_init__(self):
        super().__post_init__()
        if self.encryption is None:
            self.encryption = {"type": EncryptionType.NONE}


class ExternalVolumeStorageLocation(Resource):
    resource_type = ResourceType.EXTERNAL_VOLUME_STORAGE_LOCATION
    props = Props(
        name=StringProp("name"),
        storage_provider=EnumProp("storage_provider", ExternalVolumeStorageProvider, quoted=True),
        storage_base_url=StringProp("storage_base_url"),
        encryption=PropSet(
            "encryption",
            Props(
                type=EnumProp(
                    "type",
                    [
                        EncryptionType.AWS_SSE_S3,
                        EncryptionType.AWS_SSE_KMS,
                        EncryptionType.GCS_SSE_KMS,
                        EncryptionType.NONE,
                    ],
                    quoted=True,
                ),
                kms_key_id=StringProp("kms_key_id"),
            ),
        ),
        storage_aws_role_arn=StringProp("storage_aws_role_arn"),
        storage_aws_external_id=StringProp("storage_aws_external_id"),
    )
    scope = AnonymousScope()
    spec = _ExternalVolumeStorageLocation
    serialize_inline = True

    def __init__(
        self,
        name: str,
        storage_provider: ExternalVolumeStorageProvider,
        storage_base_url: str,
        encryption: dict = None,
        storage_aws_role_arn: str = None,
        storage_aws_external_id: str = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._data: _ExternalVolumeStorageLocation = _ExternalVolumeStorageLocation(
            name=name,
            storage_provider=storage_provider,
            storage_base_url=storage_base_url,
            encryption=encryption,
            storage_aws_role_arn=storage_aws_role_arn,
            storage_aws_external_id=storage_aws_external_id,
        )


@dataclass(unsafe_hash=True)
class _ExternalVolume(ResourceSpec):
    name: ResourceName
    owner: Role = "ACCOUNTADMIN"
    storage_locations: list[ExternalVolumeStorageLocation] = None
    allow_writes: bool = True
    comment: str = None

    def __post_init__(self):
        super().__post_init__()
        if self.storage_locations is None or len(self.storage_locations) == 0:
            raise ValueError("storage_locations is required")


class ExternalVolume(NamedResource, Resource):
    resource_type = ResourceType.EXTERNAL_VOLUME
    props = Props(
        storage_locations=PropList(
            "storage_locations",
            StructProp(ExternalVolumeStorageLocation.props),
        ),
        comment=StringProp("comment"),
    )
    scope = AccountScope()
    spec = _ExternalVolume

    def __init__(
        self,
        name: str,
        owner: str = "ACCOUNTADMIN",
        storage_locations: list[dict] = None,
        allow_writes: bool = True,
        comment: str = None,
        **kwargs,
    ):
        super().__init__(name, **kwargs)
        self._data: _ExternalVolume = _ExternalVolume(
            name=self._name,
            owner=owner,
            storage_locations=storage_locations,
            allow_writes=allow_writes,
            comment=comment,
        )
