from .base import Resource, OrganizationScoped

# from .database import Database
# from .role import Role
# from .resource_monitor import ResourceMonitor
# from .share import Share
# from .user import User
# from .warehouse import Warehouse


class Account(Resource, OrganizationScoped):
    resource_type = "ACCOUNT"

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
