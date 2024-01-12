from abc import ABC
from .identifiers import FQN


class ResourceScope(ABC):
    def fully_qualified_name(self, resource_name: str):
        raise NotImplementedError

    def register_scope(self, **kwargs):
        raise NotImplementedError


class AccountScope(ResourceScope):
    def fully_qualified_name(self, resource_name: str):
        return FQN(name=resource_name.upper())

    def register_scope(self):
        pass


class DatabaseScope(ResourceScope):
    def __init__(self):
        self.database_name = None

    def fully_qualified_name(self, resource_name: str):
        return FQN(name=resource_name.upper(), database=self.database_name)

    def register_scope(self, database=None):
        if isinstance(database, str):
            self.database_name = database.upper()


class SchemaScope(ResourceScope):
    def __init__(self):
        self.database_name = None
        self.schema_name = None

    def fully_qualified_name(self, resource_name: str):
        return FQN(name=resource_name.upper(), database=self.database_name, schema=self.schema_name)

    def register_scope(self, database=None, schema=None):
        if isinstance(database, str):
            self.database_name = database.upper()
        if isinstance(schema, str):
            self.schema_name = schema.upper()


class TableScope(ResourceScope):
    def __init__(self):
        self.database_name = None
        self.schema_name = None
        self.table_name = None

    def fully_qualified_name(self, resource_name: str):
        return FQN(
            name=resource_name.upper(),
            database=self.database_name,
            schema=self.schema_name,
            table=self.table_name,
        )

    def register_scope(self, database=None, schema=None, table=None):
        if isinstance(database, str):
            self.database_name = database.upper()
        if isinstance(schema, str):
            self.schema_name = schema.upper()
        if isinstance(table, str):
            self.table_name = table.upper()
