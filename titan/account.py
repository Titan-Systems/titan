# from .__resource import OrganizationLevelResource, AccountLevelResource, ResourceDB

from .resource import Resource, Namespace

from .database import Database
from .role import Role
from .resource_monitor import ResourceMonitor
from .share import Share
from .user import User
from .warehouse import Warehouse

from .urn import URN


class Account(Resource):
    resource_type = "ACCOUNT"
    namespace = Namespace.ORGANIZATION

    name: str
    region: str

    # def __init__(
    #     self,
    #     **kwargs,
    # ):
    #     super().__init__(**kwargs)
    #     self.databases = ResourceDB(Database)
    #     self.resource_monitors = ResourceDB(ResourceMonitor)
    #     self.roles = ResourceDB(Role)
    #     self.shares = ResourceDB(Share)
    #     self.users = ResourceDB(User)
    #     self.warehouses = ResourceDB(Warehouse)

    #     self.add(
    #         Role(name="ACCOUNTADMIN", implicit=True),
    #         Role(name="SYSADMIN", implicit=True),
    #         Role(name="USERADMIN", implicit=True),
    #         Role(name="SECURITYADMIN", implicit=True),
    #         Role(name="PUBLIC", implicit=True),
    #         Database(name="SNOWFLAKE", implicit=True),
    #     )

    @property
    def urn(self):
        """
        urn:sf:us-central1.gcp::account/AB11223
        """
        return URN(region="us-central1.gcp", resource_type="account", resource_name=self.name)

    # def add(self, *other_resources: AccountLevelResource):
    #     for other_resource in other_resources:
    #         # if not isinstance(other_resource, AccountLevelResource):
    #         #     if other_resource.namespace and other_resource.namespace != Namespace.ACCOUNT:
    #         #         raise TypeError(f"Cannot add {other_resource} to {self}")
    #         # other_resource.account = self
    #         if isinstance(other_resource, Database):
    #             self.databases[other_resource.name] = other_resource
    #         elif isinstance(other_resource, ResourceMonitor):
    #             self.resource_monitors[other_resource.name] = other_resource
    #         elif isinstance(other_resource, Role):
    #             self.roles[other_resource.name] = other_resource
    #         elif isinstance(other_resource, Share):
    #             self.shares[other_resource.name] = other_resource
    #         elif isinstance(other_resource, User):
    #             self.users[other_resource.name] = other_resource
    #         elif isinstance(other_resource, Warehouse):
    #             self.warehouses[other_resource.name] = other_resource
    #         else:
    #             raise TypeError(f"Cannot add {other_resource} to {self}")
