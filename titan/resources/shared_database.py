# from dataclasses import dataclass

# from .resource import Resource, ResourceSpec
# from ..enums import ResourceType
# from ..scope import AccountScope
# from ..props import Props, IdentifierProp


# @dataclass(unsafe_hash=True)
# class _SharedDatabase(ResourceSpec):
#     name: str
#     from_share: str
#     owner: str = "ACCOUNTADMIN"


# class SharedDatabase(Resource):
#     """
#     CREATE DATABASE <name> FROM SHARE <provider_account>.<share_name>
#     """

#     resource_type = ResourceType.DATABASE
#     props = Props(
#         from_share=IdentifierProp("from share", eq=False),
#     )
#     scope = AccountScope()
#     spec = _SharedDatabase

#     def __init__(
#         self,
#         name: str,
#         from_share: str,
#         owner: str = "ACCOUNTADMIN",
#         **kwargs,
#     ):
#         super().__init__(**kwargs)
#         self._data = _SharedDatabase(
#             name=name,
#             from_share=from_share,
#             owner=owner,
#         )
