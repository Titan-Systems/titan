from typing import Union, Optional

from .resource import AccountLevelResource, Resource

# from .user import User
# from .role import Role

Role = "Role"


class Grant(AccountLevelResource):
    pass


class UsageGrant(Grant):
    """
    UsageGrant -needs-> role
               -needs-> resource
    """

    def __init__(self, user_or_role: Role, resource: Resource, **kwargs):
        super().__init__(name="this name intentionally blank", **kwargs)
        self.grantee = user_or_role
        self.resource = resource
        self.depends_on(self.grantee, self.resource)


# class RoleGrant(Grant):


# user -needs-> RoleGrant -needs -> role
