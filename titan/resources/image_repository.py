from dataclasses import dataclass

from ..enums import AccountEdition, ResourceType
from ..props import Props
from ..resource_name import ResourceName
from ..role_ref import RoleRef
from ..scope import SchemaScope
from .resource import NamedResource, Resource, ResourceSpec


@dataclass(unsafe_hash=True)
class _ImageRepository(ResourceSpec):
    name: ResourceName
    owner: RoleRef = "SYSADMIN"


class ImageRepository(NamedResource, Resource):
    """
    Description:
        An image repository in Snowflake is a storage unit within a schema that allows for the management of OCIv2-compliant container images.

    Snowflake Docs:
        https://docs.snowflake.com/en/sql-reference/sql/create-image-repository

    Fields:
        name (string, required): The unique identifier for the image repository within the schema.
        owner (string or Role): The owner role of the image repository. Defaults to "SYSADMIN".

    Python:

        ```python
        image_repository = ImageRepository(
            name="some_image_repository",
        )
        ```

    Yaml:

        ```yaml
        image_repositories:
          - name: some_image_repository
        ```
    """

    edition = {AccountEdition.ENTERPRISE, AccountEdition.BUSINESS_CRITICAL}
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
        super().__init__(name, **kwargs)
        self._data: _ImageRepository = _ImageRepository(
            name=self._name,
            owner=owner,
        )
