from .base import Resource, AccountScoped, _fix_class_documentation
from ..props import Props, IdentifierProp
from ..privs import DatabasePriv, GlobalPriv, Privs


@_fix_class_documentation
class SharedDatabase(AccountScoped, Resource):
    """
    CREATE DATABASE <name> FROM SHARE <provider_account>.<share_name>
    """

    resource_type = "DATABASE"

    lifecycle_privs = Privs(
        create=[GlobalPriv.CREATE_DATABASE, GlobalPriv.IMPORT_SHARE],
        read=DatabasePriv.IMPORTED_PRIVILEGES,
        delete=DatabasePriv.OWNERSHIP,
    )

    props = Props(
        from_share=IdentifierProp("from share", eq=False),
    )

    name: str
    from_share: str
    owner: str = "ACCOUNTADMIN"
