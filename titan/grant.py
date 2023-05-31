from typing import Union, Optional

from .entity import AccountLevelEntity, Entity

# from .user import User
# from .role import Role

Role = "Role"


class Grant(AccountLevelEntity):
    pass


class UsageGrant(Grant):
    """
    UsageGrant -needs-> role
               -needs-> resource
    """

    def __init__(self, user_or_role: Role, resource: Entity, **kwargs):
        super().__init__(name="this name intentionally blank", **kwargs)
        self.grantee = user_or_role
        self.resource = resource
        self.depends_on(self.grantee, self.resource)


# class RoleGrant(Grant):


# user -needs-> RoleGrant -needs -> role
