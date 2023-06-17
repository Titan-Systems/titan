# from __future__ import annotations

# from typing import Union, Optional, TYPE_CHECKING

# from .resource import AccountLevelResource, Resource

# if TYPE_CHECKING:
#     from .user import User
#     from .role import Role


# class Grant(AccountLevelResource):
#     pass


# class UsageGrant(Grant):
#     """
#     UsageGrant -needs-> role
#                -needs-> resource
#     """

#     def __init__(self, user_or_role: Role, resource: Resource, **kwargs):
#         super().__init__(name="this name intentionally blank", **kwargs)
#         self.grantee = user_or_role
#         self.resource = resource
#         self.requires(self.grantee, self.resource)


# # class RoleGrant(Grant):


# # user -needs-> RoleGrant -needs -> role
