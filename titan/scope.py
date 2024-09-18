from abc import ABC

from .identifiers import FQN
from .resource_name import ResourceName


class ResourceScope(ABC):
    def fully_qualified_name(self, container, resource_name: ResourceName) -> FQN:
        raise NotImplementedError


class OrganizationScope(ResourceScope):
    def fully_qualified_name(self, _, resource_name: ResourceName) -> FQN:
        return FQN(name=resource_name)


class AccountScope(ResourceScope):
    def fully_qualified_name(self, _, resource_name: ResourceName) -> FQN:
        return FQN(name=resource_name)


class DatabaseScope(ResourceScope):
    def fully_qualified_name(self, database, resource_name: ResourceName) -> FQN:
        db = database.name if database else None
        return FQN(name=resource_name, database=db)


class SchemaScope(ResourceScope):
    def fully_qualified_name(self, schema, resource_name: ResourceName) -> FQN:
        db, sch = None, None
        if schema:
            db = schema.container.name if schema.container else None
            sch = schema.name if schema else None
        return FQN(name=resource_name, database=db, schema=sch)


class TableScope(ResourceScope):
    def fully_qualified_name(self, resource_name: ResourceName):
        raise NotImplementedError
        # return FQN(
        #     name=resource_name.upper(),
        #     database=self.database_name,
        #     schema=self.schema_name,
        #     table=self.table_name,
        # )


class AnonymousScope(ResourceScope):
    def fully_qualified_name(self, _, resource_name: ResourceName) -> FQN:
        return FQN(name=resource_name)


def resource_can_be_contained_in(resource, container):
    container_type = container.__class__.__name__
    if container_type == "ResourcePointer":
        container_type = container.resource_type.value.title()
    if (
        (isinstance(resource.scope, AccountScope) and container_type == "Account")
        or (isinstance(resource.scope, DatabaseScope) and container_type == "Database")
        or (isinstance(resource.scope, SchemaScope) and container_type == "Schema")
    ):
        return True
    return False
