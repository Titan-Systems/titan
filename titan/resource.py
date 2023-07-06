from __future__ import annotations

import re

from typing import TypeVar, Optional, Dict, Type, Set, Union, List, TYPE_CHECKING

from .props import Prop, PropList, prop_scan

# from .hooks import on_file_added_factory

if TYPE_CHECKING:
    from .account import Account
    from .database import Database
    from .schema import Schema
    from .resource_graph import ResourceGraph
    from .props import Prop

T_Resource = TypeVar("T_Resource", bound="Resource")


class ResourceDB:
    def __init__(self, cls: Type[T_Resource]):
        self.resource_class = cls
        self._db: Dict[str, T_Resource] = {}

    def __getitem__(self, key):
        if key is None:
            return None
        if key not in self._db:
            self._db[key] = self.resource_class(name=key, stub=True)
        return self._db[key]

    def __setitem__(self, key, value):
        if key is None:
            raise Exception
        if key in self._db:
            if self._db[key].stub:
                self._db[key] = value
            else:
                raise Exception
        # if key not in self._db:
        # self._db[key] = self.resource_class(name=key, implicit=True)
        # return self._db[key]
        self._db[key] = value


class ResourceWithDB(type):
    all: ResourceDB

    def __new__(cls, name, bases, attrs):
        new_cls = super().__new__(cls, name, bases, attrs)
        new_cls.all = ResourceDB(new_cls)
        return new_cls


class Resource:
    # __slots__ = ("sql", "name", "state", "_dependencies")

    # on_init = None
    level = -1
    resource_name: Optional[str] = None
    ownable = True
    create_statement: Optional[re.Pattern] = None
    props: Dict[str, Union[Prop, List[Prop]]] = {}

    def __init__(
        self,
        name: str,
        owner: Optional[str] = None,
        implicit: Optional[bool] = False,
        stub: Optional[bool] = False,
    ):
        if name is None:
            raise Exception
        self.requirements: Set[Resource] = set()
        self.required_by: Set[Resource] = set()
        self.graph: Optional[ResourceGraph] = None

        self.name = name
        self.implicit = implicit
        self.stub = stub

        self.owner = owner
        self.violates_policy = False

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

    def __repr__(self) -> str:
        return f"<{type(self).__name__}:{self.name}>"

    def __str__(self) -> str:
        return self.name

    @classmethod
    def show(cls, session):
        q = f"SHOW {cls.__name__.upper()}S"
        return [row.name for row in session.sql(q).collect()]

    @classmethod
    def parse_props(cls, sql: str):
        found_props = prop_scan(cls.__name__, cls.props, sql)

        return found_props

    @property
    def connections(self):
        return self.requirements | self.required_by

    @property
    def sql(self):
        if self.name == "my_int_stage_2":
            print("")
        props = self.props_sql()
        return f"CREATE {self.resource_name} {self.fully_qualified_name} {props}"

    def props_sql(self):
        return self._props_sql(self.props)

    def _props_sql(self, props: Dict[str, Prop]):
        sql = []
        for prop_name, prop in props.items():
            # props = prop_or_list if isinstance(prop_or_list, list) else [prop_or_list]
            # if isinstance(prop_or_list, list):
            #     raise Exception("list of props must be handled in child class")
            # prop = prop_or_list

            value = getattr(self, prop_name.lower())
            if value is None:
                continue
            rendered = prop.render(value)
            if rendered:
                sql.append(rendered)

        return " ".join(sql)

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

    def requires(self, *resources: Optional[Resource]):
        # This allows None resources but that would be a type violation
        if not any(resources):
            return
        if self.stub:
            raise Exception(f"{repr(self)} is a stub and can't require other resources")

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

    def finalize(self):
        for res in self.requirements:
            if res.stub:
                ResourceClass = res.__class__
                new_res = ResourceClass.all[res.name]
                if not new_res.stub:
                    self.requirements.remove(res)
                    self.requirements.add(new_res)

    def describe_sql(self):
        return f"DESCRIBE {self.resource_name} {self.fully_qualified_name()}"


# class Ref(Resource):
#     def __init__(self, name, resource_class):
#         self.name = name
#         self.resource_class = resource_class


class OrganizationLevelResource(Resource):
    level = 1
    ownable = False


T_AccountLevelResource = TypeVar("T_AccountLevelResource", bound="AccountLevelResource")


class AccountLevelResource(Resource, metaclass=ResourceWithDB):
    # __slots__ = ("_account")
    level = 2

    def __init__(self, *args, account: Optional[Account] = None, **kwargs):
        super().__init__(*args, **kwargs)
        self.account = account
        self.all[self.name] = self

    def __repr__(self):
        i = "🔗" if self.stub else ""
        i = "👻" if self.implicit else i
        return f"<{i}{type(self).__name__}:{self.name}>"

    @classmethod
    def from_sql(cls: Type[T_AccountLevelResource], sql: str) -> T_AccountLevelResource:
        if not cls.create_statement:
            raise Exception(f"{cls.__name__} does not have a create_statement")
        match = re.search(cls.create_statement, sql)

        if match is None:
            raise Exception
        name = match.group(1)
        props = cls.parse_props(sql[match.end() :])

        return cls(name=name, **props)

    @property
    def account(self):
        return self._account

    @account.setter
    def account(self, account_: Optional[Account]):
        self._account = account_
        if self._account is not None and not self.stub:
            self.requires(self._account)

    @property
    def fully_qualified_name(self):
        return self.name.upper()


T_DatabaseLevelResource = TypeVar("T_DatabaseLevelResource", bound="DatabaseLevelResource")


class DatabaseLevelResource(Resource, metaclass=ResourceWithDB):
    # __slots__ = ("_database")
    level = 3

    def __init__(self, database: Optional[Database] = None, **kwargs):
        super().__init__(**kwargs)
        self.database = database

    def __repr__(self):
        i = "🔗" if self.stub else ""
        i = "👻" if self.implicit else i
        db = self.database.name if self.database else ""
        return f"<{i}{type(self).__name__}:{db}.{self.name}>"

    @classmethod
    def from_sql(cls: Type[T_DatabaseLevelResource], sql: str) -> T_DatabaseLevelResource:
        if not cls.create_statement:
            raise Exception(f"{cls.__name__} does not have a create_statement")
        # There needs to be conflict resolution here
        match = re.search(cls.create_statement, sql)

        if match is None:
            raise Exception
        name = match.group(1)
        props = cls.parse_props(sql[match.end() :])
        return cls(name=name, **props)

    @property
    def database(self) -> Optional[Database]:
        return self._database

    @database.setter
    def database(self, database_: Optional[Database]):
        self._database = database_
        if self._database is not None:
            self.requires(self._database)

    @property
    def fully_qualified_name(self):
        database = self.database.name if self.database else "[ NULL ]"
        name = self.name.upper()
        return f"{database}.{name}"


T_SchemaLevelResource = TypeVar("T_SchemaLevelResource", bound="SchemaLevelResource")


class SchemaLevelResource(Resource):
    # __slots__ = ("_schema")
    level = 4

    def __init__(self, schema: Optional[Schema] = None, **kwargs):
        super().__init__(**kwargs)
        self.schema = schema

    def __repr__(self):
        i = "🔗" if self.stub else ""
        i = "👻" if self.implicit else i
        db, schema = "", ""
        if self.schema:
            schema = self.schema.name
            if self.schema.database:
                db = self.schema.database.name
        return f"<{i}{type(self).__name__}:{db}.{schema}.{self.name}>"

    @classmethod
    def from_sql(cls: Type[T_SchemaLevelResource], sql: str) -> T_SchemaLevelResource:
        if not cls.create_statement:
            raise Exception(f"{cls.__name__} does not have a create_statement")
        # There needs to be conflict resolution here
        match = re.search(cls.create_statement, sql)

        if match is None:
            raise Exception
        name = match.group(1)
        props = cls.parse_props(sql[match.end() :])
        return cls(name=name, **props)

    @property
    def database(self) -> Optional[Database]:
        if self.schema is not None:
            return self.schema.database
        return None

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

    @property
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
