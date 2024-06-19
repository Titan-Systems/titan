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
    CREATE [ OR REPLACE ] TAG [ IF NOT EXISTS ] <name> [ COMMENT = '<string_literal>' ]

    CREATE [ OR REPLACE ] TAG [ IF NOT EXISTS ] <name>
        [ ALLOWED_VALUES '<val_1>' [ , '<val_2>' , [ ... ] ] ]
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
