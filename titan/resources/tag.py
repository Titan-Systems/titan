from dataclasses import dataclass
from typing import Optional, TYPE_CHECKING

from ..enums import AccountEdition, ResourceType
from ..identifiers import FQN
from ..props import Props, StringListProp, StringProp
from ..resource_name import ResourceName
from ..resource_tags import ResourceTags
from ..scope import AccountScope, SchemaScope
from .resource import NamedResource, Resource, ResourcePointer, ResourceSpec

if TYPE_CHECKING:
    from .role import Role


@dataclass(unsafe_hash=True)
class _Tag(ResourceSpec):
    name: ResourceName
    owner: "Role" = "SYSADMIN"
    comment: str = None
    allowed_values: list = None


class Tag(NamedResource, Resource):
    """
    Description:
        Represents a tag in Snowflake, which can be used to label various resources for better management and categorization.

    Snowflake Docs:
        https://docs.snowflake.com/en/sql-reference/sql/create-tag

    Fields:
        name (string, required): The name of the tag.
        allowed_values (list): A list of allowed values for the tag.
        comment (string): A comment or description for the tag.

    Python:

        ```python
        tag = Tag(
            name="cost_center",
            allowed_values=["finance", "engineering", "sales"],
            comment="This is a sample tag",
        )
        ```

    Yaml:

        ```yaml
        tags:
          - name: cost_center
            comment: This is a sample tag
            allowed_values:
              - finance
              - engineering
              - sales
        ```
    """

    edition = {AccountEdition.ENTERPRISE, AccountEdition.BUSINESS_CRITICAL}
    resource_type = ResourceType.TAG
    props = Props(
        allowed_values=StringListProp("allowed_values", eq=False, parens=False),
        comment=StringProp("comment"),
    )
    scope = SchemaScope()
    spec = _Tag

    def __init__(
        self,
        name: str,
        owner: str = "SYSADMIN",
        comment: str = None,
        allowed_values: list = None,
        **kwargs,
    ):
        super().__init__(name, **kwargs)
        self._data: _Tag = _Tag(
            name=self._name,
            owner=owner,
            comment=comment,
            allowed_values=allowed_values,
        )


@dataclass(unsafe_hash=True)
class _TagReference(ResourceSpec):
    object_name: str
    object_domain: ResourceType
    tags: ResourceTags


class TagReference(Resource):

    edition = {AccountEdition.ENTERPRISE, AccountEdition.BUSINESS_CRITICAL}
    resource_type = ResourceType.TAG_REFERENCE
    props = Props()
    scope = AccountScope()
    spec = _TagReference

    def __init__(
        self,
        object_name: str,
        object_domain: str,
        tags: dict[str, str],
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._data: _TagReference = _TagReference(
            object_name=object_name,
            object_domain=object_domain,
            tags=tags,
        )

    @property
    def fqn(self):
        return tag_reference_fqn(self._data)

    @property
    def tags(self) -> Optional[ResourceTags]:
        return self._data.tags


def tag_reference_fqn(data: _TagReference) -> FQN:
    return FQN(
        name=ResourceName(data.object_name),
        params={
            "domain": data.object_domain,
        },
    )


class TaggableResource:
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._tags: Optional[ResourceTags] = None

    def set_tags(self, tags: dict[str, str]):
        if tags is None:
            return
        if self._tags is None:
            self._tags = ResourceTags(tags)
        else:
            raise ValueError("Tags cannot be set on a resource that already has tags")

    def create_tag_reference(self):
        if self._tags is None:
            return None
        ref = TagReference(
            object_name=str(self.fqn),
            object_domain=self.resource_type,
            tags=self._tags,
        )
        ref.requires(self)
        for tag in self._tags.keys():
            tag_ptr = ResourcePointer(name=tag, resource_type=ResourceType.TAG)
            ref.requires(tag_ptr)
        return ref

    @property
    def tags(self) -> Optional[ResourceTags]:
        return self._tags
