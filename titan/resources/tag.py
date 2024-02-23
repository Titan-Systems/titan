from dataclasses import dataclass

from .resource import Resource, ResourceSpec
from ..enums import AccountEdition, ResourceType
from ..scope import SchemaScope
from ..props import Props, StringProp, StringListProp


@dataclass(unsafe_hash=True)
class _Tag(ResourceSpec):
    name: str
    comment: str = None
    allowed_values: list = None


class Tag(Resource):
    """
    CREATE [ OR REPLACE ] TAG [ IF NOT EXISTS ] <name> [ COMMENT = '<string_literal>' ]

    CREATE [ OR REPLACE ] TAG [ IF NOT EXISTS ] <name>
        [ ALLOWED_VALUES '<val_1>' [ , '<val_2>' , [ ... ] ] ]
    """

    edition = {AccountEdition.ENTERPRISE, AccountEdition.BUSINESS_CRITICAL}
    resource_type = ResourceType.TAG
    props = Props(
        comment=StringProp("comment"),
        allowed_values=StringListProp("allowed_values", eq=False),
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
        super().__init__(**kwargs)
        self._data: _Tag = _Tag(
            name=name,
            comment=comment,
            allowed_values=allowed_values,
        )
