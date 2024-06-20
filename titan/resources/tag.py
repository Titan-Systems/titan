from dataclasses import dataclass

from .resource import Resource, ResourceSpec, ResourceNameTrait
from .role import Role
from ..enums import AccountEdition, ResourceType
from ..scope import SchemaScope
from ..props import Props, StringProp, StringListProp
from ..resource_name import ResourceName


@dataclass(unsafe_hash=True)
class _Tag(ResourceSpec):
    name: ResourceName
    comment: str = None
    allowed_values: list = None


class Tag(ResourceNameTrait, Resource):
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
        comment: str = None,
        allowed_values: list = None,
        **kwargs,
    ):
        super().__init__(name, **kwargs)
        self._data: _Tag = _Tag(
            name=self._name,
            comment=comment,
            allowed_values=allowed_values,
        )
