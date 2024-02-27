from abc import ABC
from .identifiers import FQN


class ResourceScope(ABC):
    def fully_qualified_name(self, container, resource_name: str) -> FQN:
        raise NotImplementedError


class OrganizationScope(ResourceScope):
    def fully_qualified_name(self, _, resource_name: str) -> FQN:
        return FQN(name=resource_name)


class AccountScope(ResourceScope):
    def fully_qualified_name(self, _, resource_name: str) -> FQN:
        return FQN(name=resource_name)


class DatabaseScope(ResourceScope):
    def fully_qualified_name(self, database, resource_name: str) -> FQN:
        db = database.name if database else None
        return FQN(name=resource_name, database=db)


class SchemaScope(ResourceScope):
    def fully_qualified_name(self, schema, resource_name: str) -> FQN:
        db, sch = None, None
        if schema:
            db = schema.container.name if schema.container else None
            sch = schema.name if schema else None
        return FQN(name=resource_name, database=db, schema=sch)


class TableScope(ResourceScope):
    def fully_qualified_name(self, resource_name: str):
        raise NotImplementedError
        # return FQN(
        #     name=resource_name.upper(),
        #     database=self.database_name,
        #     schema=self.schema_name,
        #     table=self.table_name,
        # )
