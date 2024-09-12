from dataclasses import dataclass
from typing import Optional

from ..enums import AccountEdition, ResourceType
from ..identifiers import FQN
from ..props import Props, StringListProp, StringProp
from ..resource_name import ResourceName
from ..resource_tags import ResourceTags
from ..role_ref import RoleRef
from ..scope import AccountScope, SchemaScope
from .resource import NamedResource, Resource, ResourcePointer, ResourceSpec


@dataclass(unsafe_hash=True)
class _Tag(ResourceSpec):
    name: ResourceName
    owner: RoleRef = "SYSADMIN"
    comment: str = None
    allowed_values: list = None

    def __post_init__(self):
        super().__post_init__()
        if self.allowed_values is not None:
            self.allowed_values.sort()


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
        for tag in tags.keys():
            tag_ptr = ResourcePointer(name=tag, resource_type=ResourceType.TAG)
            self.requires(tag_ptr)

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
            "domain": str(data.object_domain),
        },
    )


def tag_reference_for_resource(resource: Resource, tags: dict[str, str]) -> TagReference:
    return TagReference(
        object_name=str(resource.fqn),
        object_domain=resource.resource_type,
        tags=tags,
    )


class TaggableResource:
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._tags: Optional[ResourceTags] = None

    def set_tags(self, tags: Optional[dict[str, str]]):
        if tags is None:
            return
        if self._tags is None:
            self._tags = ResourceTags(tags)
        else:
            raise ValueError("Tags cannot be set on a resource that already has tags")

    def create_tag_reference(self):
        if self._tags is None:
            return None
        ref = tag_reference_for_resource(self, self._tags)
        ref.requires(self)
        return ref

    @property
    def tags(self) -> Optional[ResourceTags]:
        return self._tags
