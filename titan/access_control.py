from typing import List

from pydantic import BaseModel

from .resources import Resource, Grant
from .resources.role import T_Role
from .enums import ParseableEnum


class SuperPriv(ParseableEnum):
    READ = "READ"
    WRITE = "WRITE"
    CREATE = "CREATE"
    DELETE = "DELETE"


def _resolve_privs(super_priv, resource_or_path) -> list:
    if isinstance(resource_or_path, Resource):
        resource = resource_or_path
    if super_priv == SuperPriv.READ:
        return resource.lifecycle_privs.read
    elif super_priv == SuperPriv.WRITE:
        return resource.lifecycle_privs.write
    elif super_priv == SuperPriv.CREATE:
        return resource.lifecycle_privs.create
    elif super_priv == SuperPriv.DELETE:
        return resource.lifecycle_privs.delete


def _is_ownership_priv(priv):
    return priv.value == "OWNERSHIP"


class ACL(BaseModel):
    privs: List[SuperPriv]
    roles: List[T_Role]
    resources: List[Resource]

    def grants(self):
        """Return a list of grants that this ACL represents."""
        grants = []
        for resource in self.resources:
            for role in self.roles:
                for super_priv in self.privs:
                    privs = _resolve_privs(super_priv, resource)
                    for priv in privs:
                        if _is_ownership_priv(priv):
                            # grants.append(OwnershipGrant(on=resource, to=role))
                            raise Exception("OwnershipGrant is deprecated")
                        else:
                            grants.append(Grant(priv=priv, on=resource, to=role))
        return grants
