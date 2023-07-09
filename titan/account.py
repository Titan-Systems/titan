from enum import Enum

from typing import Union, Optional

from .resource import OrganizationLevelResource, AccountLevelResource, ResourceDB

from .database import Database
from .role import Role
from .resource_monitor import ResourceMonitor
from .share import Share
from .user import User
from .warehouse import Warehouse


class Account(OrganizationLevelResource):
    resource_name = "ACCOUNT"

    def __init__(
        self,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.databases = ResourceDB(Database)
        self.resource_monitors = ResourceDB(ResourceMonitor)
        self.roles = ResourceDB(Role)
        self.shares = ResourceDB(Share)
        self.users = ResourceDB(User)
        self.warehouses = ResourceDB(Warehouse)

        # self.roles["ACCOUNTADMIN"] = Role(name="ACCOUNTADMIN", implicit=True)
        # self.roles["SYSADMIN"] = Role(name="SYSADMIN", implicit=True)
        # self.roles["USERADMIN"] = Role(name="USERADMIN", implicit=True)
        # self.roles["SECURITYADMIN"] = Role(name="SECURITYADMIN", implicit=True)
        # self.roles["PUBLIC"] = Role(name="PUBLIC", implicit=True)
        self.add(
            Role(name="ACCOUNTADMIN", implicit=True),
            Role(name="SYSADMIN", implicit=True),
            Role(name="USERADMIN", implicit=True),
            Role(name="SECURITYADMIN", implicit=True),
            Role(name="PUBLIC", implicit=True),
            Database(name="SNOWFLAKE", implicit=True),
        )

    def add(self, *other_resources: AccountLevelResource):
        for other_resource in other_resources:
            if not isinstance(other_resource, AccountLevelResource):
                raise TypeError(f"Cannot add {other_resource} to {self}")
            other_resource.account = self
            if isinstance(other_resource, Database):
                self.databases[other_resource.name] = other_resource
            elif isinstance(other_resource, ResourceMonitor):
                self.resource_monitors[other_resource.name] = other_resource
            elif isinstance(other_resource, Role):
                self.roles[other_resource.name] = other_resource
            elif isinstance(other_resource, Share):
                self.shares[other_resource.name] = other_resource
            elif isinstance(other_resource, User):
                self.users[other_resource.name] = other_resource
            elif isinstance(other_resource, Warehouse):
                self.warehouses[other_resource.name] = other_resource
            else:
                raise TypeError(f"Cannot add {other_resource} to {self}")
