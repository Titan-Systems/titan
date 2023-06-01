import inspect
import typing as t

from contextlib import contextmanager
from collections import defaultdict
from queue import Queue
from enum import Enum, auto

from titan.parser import parse_names
from titan.hooks import on_file_added_factory

# from titan.props import PROPS


class Resource:
    # __slots__ = ("sql", "name", "state", "_dependencies")

    on_init = None
    level = -1

    def __init__(self, name: str, query_text=None, implicit=False, owner=None):
        self.dependencies = []
        self.graph = None
        self.state = {}

        self.name = name
        self.query_text = query_text
        self.implicit = implicit

        self.owner = owner

        if self.on_init:
            self.on_init(self)

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

    def create(self, session):
        if self.implicit:
            print("########", "implicit", self.name)
            return
        print("~" * 8, type(self).__name__, self.name)
        if self.query_text:
            session.sql(self.query_text).collect()  # block=False
        else:
            print("!!!!! Creation Failed, no SQL to run", self.name)

    def depends_on(self, *resources):
        for resource in resources:
            if not isinstance(resource, Resource):
                raise Exception(f"[{resource}:{type(resource)}] is not an Resource")
            self.dependencies.append(resource)

    def fully_qualified_name(self):
        raise NotImplementedError

    # def add(self, other_resource):
    #     if self.level >= other_resource.level:
    #         raise Exception(f"{other_resource.__repr__()} can't be added to {self.__repr__()} ")
    #     # If self has already been registered in a graph, the graph owns this responsibility
    #     if self.graph:
    #         self.graph.


class OrganizationLevelResource(Resource):
    level = 1


class AccountLevelResource(Resource):
    level = 2

    @classmethod
    def sql(cls, query_text: str):
        _, _, name = parse_names(query_text)
        return cls(name=name, query_text=query_text)

    def fully_qualified_name(self):
        return self.name.upper()


class DatabaseLevelResource(Resource):
    level = 3

    def __init__(self, *args, database=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.database = database

    @classmethod
    def sql(cls, query_text: str):
        database, _, name = parse_names(query_text)
        return cls(name=name, query_text=query_text)

    @property
    def database(self):
        return self._database

    @database.setter
    def database(self, database_):
        self._database = database_
        if database_:
            self.depends_on(self._database)

    def fully_qualified_name(self):
        database = self.database.name if self.database else "[DB]"
        name = self.name.upper()
        return f"{database}.{name}"


class SchemaLevelResource(Resource):
    level = 4

    def __init__(self, database=None, schema=None, **kwargs):
        super().__init__(**kwargs)
        self.database = database
        self.schema = schema

    @classmethod
    def from_sql(cls, query_text: str, **kwargs):
        # There needs to be conflict resolution here
        database, schema, name = parse_names(query_text)
        return cls(name=name, query_text=query_text, database=database, schema=schema, **kwargs)

    @property
    def database(self):
        return self._database

    @database.setter
    def database(self, database_):
        self._database = database_
        if database_:
            self.depends_on(self._database)

    @property
    def schema(self):
        return self._schema

    @schema.setter
    def schema(self, schema_):
        self._schema = schema_
        if schema_:
            self.depends_on(self._schema)

    def fully_qualified_name(self):
        database = self.database.name if self.database else "[DB]"
        schema = self.schema.name if self.schema else "[SCHEMA]"
        name = self.name.upper()
        return f"{database}.{schema}.{name}"

    def add(self, other_resource):
        raise Exception(f"{self.__repr__()} can't add other resources")


class View(SchemaLevelResource):
    pass


class Sproc(SchemaLevelResource):
    pass
    # @classmethod
    # def func(cls, func_):
    #     # database, schema, name = parse_names(query_text)
    #     return cls(name=name, query_text=query_text)


class Stage(SchemaLevelResource):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.hooks = {"on_file_added": None}

    @property
    def on_file_added(self):
        return self.hooks["on_file_added"]

    @on_file_added.setter
    def on_file_added(self, hook):
        # TODO: This needs to be refactored to be wrapped in a Sproc resource and for dependencies to be implicitly tracked
        self.hooks["on_file_added"] = on_file_added_factory("ZIPPED_TRIPS", hook)
        self.state["on_file_added:last_checked"] = State(key="last_checked", value="'1900-01-01'::DATETIME")
        # print("on_file_added")
        # print(inspect.getsource(self.hooks["on_file_added"]))

    def create(self, session):
        super().create(session)

        for statefunc in self.state.values():
            statefunc.create(session)

        if self.hooks["on_file_added"]:
            session.sql("CREATE STAGE IF NOT EXISTS sprocs").collect()
            session.add_packages("snowflake-snowpark-python")
            session.sproc.register(
                self.hooks["on_file_added"],
                name=f"ZIPPED_TRIPS_hook_on_file_added",
                replace=True,
                is_permanent=True,
                stage_location="@sprocs",
                execute_as="caller",
            )


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


class ResourceGraph:
    """
    The ResourceGraph is a DAG that manages dependent relationships between Titan resources.

    For example: a table, like most resources, must have a schema and database. The resource graph
    represents this as
    [table] -needs-> [schema] -needs-> [database]

    ```
    @app.table()
    def one_off():
        return "create table STAGING.ONE_OFF (...)"
    ````

    ```
    @app.table(schema="STAGING")
    def one_off():
        return "create table ONE_OFF (...)"
    ````

    ```
    with app.schema("STAGING") as schema:
        titan.table("create table ONE_OFF (...)")
    ```

    """

    def __init__(self):
        self._graph = {}
        self._in_degree = {}
        self._ref_listener = None
        # self.pending_refs = []

    def __len__(self):
        return len(self._graph.keys())

    @property
    def all(self) -> t.List[Resource]:
        return list(self._graph.keys())

        # While func is executed, the __format__ function for one or more resources may be called
        # we need to find a way to bubble that up so that this View resource is dependent on those
        # other resources

    def add(self, *resources: Resource):
        """

        There are many ways that resource relationships can be captured, both explicitly and implicitly.


        # Direct approach
        app = titan.App(database="RAW")
        t = titan.Table(name="foo", columns=[...])
        app.add(t)

        # Function Decorator
        app = titan.App(database="RAW")
        @app.table()
        def t():
            return "CREATE TABLE foo (...)"

        # Ridealong
        d = titan.Database(name="RAW")
        t = titan.Table(name="foo", columns=[...])
        d.add(t)
        app.add(d)

        # Interpolated
        @app.view()
        def v(t):
            return "CREATE VIEW bar AS SELECT * FROM {t}"


        # Session context
        app.from_sql(`
            CREATE DATABASE RAW;
            CREATE TABLE foo (...);
        `)



        """

        for resource in resources:
            if resource in self._graph:
                return

            self._graph[resource] = set()
            self._in_degree[resource] = 0
            resource.graph = self

            if self._ref_listener:
                for ref in self._ref_listener:
                    self.add_dependency(resource, ref)

            for dep in resource.dependencies:
                self.add_dependency(resource, dep)

    def add_dependency(self, resource, dependency):
        # [resource] -needs-> [dependency]

        # I'm trying to figure out if ResourcePointers make sense in this system, and if there
        # should exist a way where they start as pointers but get resolved into objects.
        # This is common for DBs and schemas that might live in existing code

        self.add(dependency)
        if dependency not in self._graph[resource]:
            self._graph[resource].add(dependency)
            self._in_degree[dependency] += 1

    def notify(self, resource):
        """
        An resource has just notified us that it is being interpolated. Add it to a list of items to
        tack on as dependencies
        """
        # self.pending_refs.append(resource)
        pass

    def sorted(self):
        # Kahn's algorithm
        print("^" * 120)
        for node, edges in self._graph.items():
            for edge in edges:
                print(node, "-needs->", edge)

        # Compute in-degree (# of inbound edges) for each node
        graph = self._graph.copy()
        in_degrees = self._in_degree.copy()

        print("inbound edges", in_degrees)

        # Put all nodes with 0 in-degree in a queue
        queue = Queue()
        for node, in_degree in in_degrees.items():
            if in_degree == 0:
                queue.put(node)

        # Create an empty node list
        nodes = []

        while not queue.empty():
            node = queue.get()
            nodes.append(node)

            # For each of node's outgoing edges
            empty_neighbors = set()
            for neighbor in graph[node]:
                in_degrees[neighbor] -= 1
                if in_degrees[neighbor] == 0:
                    queue.put(neighbor)
                    # graph[node].remove(neighbor)
                    empty_neighbors.add(neighbor)

            graph[node].difference_update(empty_neighbors)
        print("^" * 120)
        nodes.reverse()
        return nodes

    class ReferenceListener:
        # This is a dumb name
        pass
        # def __init__(self):
        #     self.

    @contextmanager
    def capture_refs(self):
        # This is a context manager that yields a special context that will auto link any references

        if self._ref_listener:
            raise Exception("Only one reference listener can be active at a time")

        print(">&>&> started listening")
        self._ref_listener = set()
        yield self
        self._ref_listener = None
        print(">&>&> ended listening")

    def resource_referenced(self, resource):
        print(">&>&> resource referenced")
        if self._ref_listener is not None:
            print(">&>&> resource ref added")
            self._ref_listener.add(resource)
