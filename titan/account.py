from enum import Enum

from typing import Union, Optional

from .resource import OrganizationLevelResource

from .role import Role


class Account(OrganizationLevelResource):
    """ """

    def __init__(
        self,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.accountadmin = Role(name="ACCOUNTADMIN", implicit=True)
        self.sysadmin = Role(name="SYSADMIN", implicit=True)
        self.useradmin = Role(name="USERADMIN", implicit=True)
        self.securityadmin = Role(name="SECURITYADMIN", implicit=True)
