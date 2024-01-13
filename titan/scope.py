from abc import ABC
from .identifiers import FQN


class ResourceScope(ABC):
    def fully_qualified_name(self, container, resource_name: str):
        raise NotImplementedError


class OrganizationScope(ResourceScope):
    def fully_qualified_name(self, _, resource_name: str):
        return FQN(name=resource_name.upper())


class AccountScope(ResourceScope):
    def fully_qualified_name(self, _, resource_name: str):
        return FQN(name=resource_name.upper())


class DatabaseScope(ResourceScope):
    def fully_qualified_name(self, database, resource_name: str):
        return FQN(name=resource_name.upper(), database=database.name.upper())


class SchemaScope(ResourceScope):
    def fully_qualified_name(self, schema, resource_name: str):
        database = schema.container.name.upper() if schema.container else None
        return FQN(
            name=resource_name.upper(),
            database=database,
            schema=schema.name.upper(),
        )


class TableScope(ResourceScope):
    def fully_qualified_name(self, resource_name: str):
        raise NotImplementedError
        # return FQN(
        #     name=resource_name.upper(),
        #     database=self.database_name,
        #     schema=self.schema_name,
        #     table=self.table_name,
        # )
