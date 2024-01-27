from dataclasses import dataclass

from .resource import Resource, ResourceSpec
from ..enums import AccountEdition, ResourceType
from ..scope import AccountScope
from ..props import Props, StringProp, StringListProp


@dataclass
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

    resource_type = ResourceType.TAG
    props = Props(
        comment=StringProp("comment"),
        allowed_values=StringListProp("allowed_values", eq=False),
    )
    # requires = {AccountEdition.ENTERPRISE}
    scope = AccountScope()
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
