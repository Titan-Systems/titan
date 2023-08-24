from typing import List, Union

from pydantic import BaseModel

from .resources import Resource, Grant
from .resources.role import T_Role
from .enums import ParseableEnum, GlobalPriv


class SuperPriv(ParseableEnum):
    READ = "READ"
    WRITE = "WRITE"
    CREATE = "CREATE"
    DELETE = "DELETE"


def resolve_privs(super_priv, resource_or_path):
    if isinstance(resource_or_path, Resource):
        resource = resource_or_path
    if super_priv == SuperPriv.READ:
        return resource.lifecycle.read
    elif super_priv == SuperPriv.WRITE:
        return resource.lifecycle.write
    elif super_priv == SuperPriv.CREATE:
        return resource.lifecycle.create
    elif super_priv == SuperPriv.DELETE:
        return resource.lifecycle.delete


class ACL(BaseModel):
    privs: List[SuperPriv]
    roles: List[T_Role]
    resources: List[Resource]

    def grants(self):
        """Return a list of grants that this ACL represents."""
        return [
            Grant(privs=resolve_privs(priv, resource), on=resource, to=role)
            for priv in self.privs
            for role in self.roles
            for resource in self.resources
        ]
