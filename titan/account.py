from enum import Enum

from typing import Union, Optional

from .resource import OrganizationLevelResource, ResourceDB

from .role import Role


class Account(OrganizationLevelResource):
    def __init__(
        self,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.roles = ResourceDB(Role)
        self.roles["ACCOUNTADMIN"] = Role(name="ACCOUNTADMIN", implicit=True)
        self.roles["SYSADMIN"] = Role(name="SYSADMIN", implicit=True)
        self.roles["USERADMIN"] = Role(name="USERADMIN", implicit=True)
        self.roles["SECURITYADMIN"] = Role(name="SECURITYADMIN", implicit=True)
        self.roles["PUBLIC"] = Role(name="PUBLIC", implicit=True)
