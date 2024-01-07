from .base import AccountScoped, Resource, _fix_class_documentation
from ..props import Props, StringProp, StringListProp


@_fix_class_documentation
class Tag(AccountScoped, Resource):
    """
    CREATE [ OR REPLACE ] TAG [ IF NOT EXISTS ] <name> [ COMMENT = '<string_literal>' ]

    CREATE [ OR REPLACE ] TAG [ IF NOT EXISTS ] <name>
        [ ALLOWED_VALUES '<val_1>' [ , '<val_2>' , [ ... ] ] ]
    """

    resource_type = "TAG"
    props = Props(
        comment=StringProp("comment"),
        allowed_values=StringListProp("allowed_values", eq=False),
    )

    name: str
    comment: str = None
    allowed_values: list = None
