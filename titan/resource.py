from __future__ import annotations

import inspect

from collections import defaultdict
from enum import Enum, auto
from typing import TypeVar, Optional, List, Type, Set, TYPE_CHECKING

from .parser import parse_names
from .hooks import on_file_added_factory

if TYPE_CHECKING:
    from .account import Account
    from .database import Database
    from .schema import Schema
    from .resource_graph import ResourceGraph


class Resource:
    # __slots__ = ("sql", "name", "state", "_dependencies")

    on_init = None
    level = -1

    def __init__(self, name: str, query_text=None, implicit=False, owner=None):
        if name is None:
            raise Exception
        self.requirements: Set[Resource] = set()
        self.required_by: Set[Resource] = set()
        self.graph: Optional[ResourceGraph] = None

        self.name = name
        self.query_text = query_text
        self.implicit = implicit

        self.owner = owner

        # FIXME: This is halfway through object initialization
        # if self.on_init:
        #     self.on_init(self)

    def __format__(self, format_spec):
        # IDEA: Maybe titan should support some format_spec options like mytable:qualified
        # print("__format__", self.__class__, format_spec, self.name, self.graph is None)
        # print("^^^^^", inspect.currentframe().f_back.f_back.f_code.resource_cls)

        if self.graph and format_spec != "raw":
            self.graph.resource_referenced(self)

        return self.fully_qualified_name()

    @classmethod
    def show(cls, session):
        q = f"SHOW {cls.__name__.upper()}S"
        return [row.name for row in session.sql(q).collect()]

    def __repr__(self):
        return f"{type(self).__name__}:{self.name}"

    @property
    def connections(self):
        return self.requirements | self.required_by

    def create(self, session):
        raise NotImplementedError
        # if self.implicit:
        #     print("########", "implicit", self.name)
        #     return
        # print("~" * 8, type(self).__name__, self.name)
        # if self.query_text:
        #     session.sql(self.query_text).collect()  # block=False
        # else:
        #     print("!!!!! Creation Failed, no SQL to run", self.name)

    def requires(self, *resources: Resource):
        for resource in resources:
            if not isinstance(resource, Resource):
                raise Exception(f"[{resource}:{type(resource)}] is not an Resource")
            self.requirements.add(resource)
            resource.required_by.add(self)
            # FIXME: This is code smell
            if self.graph and resource.graph is None:
                self.graph.add(resource)

    def fully_qualified_name(self):
        raise NotImplementedError

    def add(self, other_resource):
        if self.level >= other_resource.level:
            raise Exception(f"{repr(other_resource)} can't be added to {repr(self)} ")


class OrganizationLevelResource(Resource):
    level = 1


class AccountLevelResource(Resource):
    # __slots__ = ("_account")
    level = 2

    def __init__(self, account: Optional[Account] = None, **kwargs):
        super().__init__(**kwargs)
        self.account = account

    @classmethod
    def sql(cls, query_text: str):
        _, _, name = parse_names(query_text)
        return cls(name=name, query_text=query_text)

    @property
    def account(self):
        return self._account

    @account.setter
    def account(self, account_: Optional[Account]):
        self._account = account_
        if self._account is not None:
            self.requires(self._account)

    def fully_qualified_name(self):
        return self.name.upper()


class DatabaseLevelResource(Resource):
    # __slots__ = ("_database")
    level = 3

    def __init__(self, database: Optional[Database] = None, **kwargs):
        super().__init__(**kwargs)
        self.database = database

    @classmethod
    def sql(cls, query_text: str):
        database, _, name = parse_names(query_text)
        return cls(name=name, query_text=query_text)

    @property
    def database(self) -> Optional[Database]:
        return self._database

    @database.setter
    def database(self, database_: Optional[Database]):
        self._database = database_
        if self._database is not None:
            self.requires(self._database)

    def fully_qualified_name(self):
        database = self.database.name if self.database else "[ NULL ]"
        name = self.name.upper()
        return f"{database}.{name}"


class SchemaLevelResource(Resource):
    # __slots__ = ("_schema")
    level = 4

    def __init__(self, schema: Optional[Schema] = None, **kwargs):
        super().__init__(**kwargs)
        self.schema = schema

    @classmethod
    def from_sql(cls, query_text: str, **kwargs):
        # There needs to be conflict resolution here
        database, schema, name = parse_names(query_text)
        return cls(name=name, query_text=query_text, database=database, schema=schema, **kwargs)

    @property
    def database(self) -> Optional[Database]:
        if self.schema is not None:
            return self.schema.database

    @database.setter
    def database(self, database_):
        raise NotImplementedError

    @property
    def schema(self) -> Optional[Schema]:
        return self._schema

    @schema.setter
    def schema(self, schema_: Optional[Schema]):
        self._schema = schema_
        if self._schema is not None:
            self.requires(self._schema)

    def fully_qualified_name(self):
        database = self.database.name if self.database else "[DB]"
        schema = self.schema.name if self.schema else "[SCHEMA]"
        name = self.name.upper()
        return f"{database}.{schema}.{name}"

    def add(self, other_resource: Resource):
        raise Exception(f"{repr(self)} can't add other resources")


class Function(SchemaLevelResource):
    pass


class State(Function):
    def __init__(self, key, value):
        self.name = f"stage_state_on_file_added_{key}"
        returns = value.split("::")[-1]
        self.sql = f"""
        CREATE FUNCTION {self.name}()
          returns {returns}
          language SQL
          IMMUTABLE
          MEMOIZABLE
        as $$
        SELECT {value}
        $$;
        """


class Pipe(SchemaLevelResource):
    pass


class FileFormat(SchemaLevelResource):
    pass


# class ResourcePointer(Resource):
#     @classmethod
#     def show(cls, session):
#         return []

#     def create(self, session):
#         return


# class NullResource(Resource):
#     def __init__(self):
#         pass
