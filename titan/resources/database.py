from .base import Resource, AccountScoped, _fix_class_documentation
from ..props import Props, IdentifierProp


@_fix_class_documentation
class SharedDatabase(Resource, AccountScoped):
    """
    CREATE DATABASE <name> FROM SHARE <provider_account>.<share_name>
    """

    resource_type = "DATABASE"
    props = Props(
        from_share=IdentifierProp("from share", eq=False),
    )

    name: str
    from_share: str
