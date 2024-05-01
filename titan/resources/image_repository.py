from dataclasses import dataclass

from .resource import Resource, ResourceSpec
from ..enums import ResourceType
from ..props import Props
from ..resource_name import ResourceName
from ..scope import SchemaScope


@dataclass(unsafe_hash=True)
class _ImageRepository(ResourceSpec):
    name: ResourceName
    owner: str = "SYSADMIN"


class ImageRepository(Resource):
    """An image repository is an OCIv2-compliant image registry service and a storage unit call repository to store images.

    CREATE [ OR REPLACE ] IMAGE REPOSITORY [ IF NOT EXISTS ] <name>
    """

    resource_type = ResourceType.IMAGE_REPOSITORY
    props = Props()
    scope = SchemaScope()
    spec = _ImageRepository

    def __init__(
        self,
        name: str,
        owner: str = "SYSADMIN",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._data = _ImageRepository(
            name=name,
            owner=owner,
        )
