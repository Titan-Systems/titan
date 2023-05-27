import inspect
import typing as t

from contextlib import contextmanager
from collections import defaultdict
from queue import Queue
from enum import Enum, auto

from titan.parser import parse_names
from titan.hooks import on_file_added_factory

from titan.props import PROPS


class Entity:
    # __slots__ = ("sql", "name", "state", "_dependencies")

    PROPS = {}

    def __init__(self, name: str, query_text=None, implicit=False, **props_kwargs):
        self.dependencies = []
        self.graph = None
        self.state = {}

        self.name = name
        self.query_text = query_text
        self.implicit = implicit
        for k, v in props_kwargs.items():
            if k.lower() not in self.PROPS:
                raise TypeError(f"__init__() got an unexpected keyword argument '{k}'")
            setattr(self, k, v)

    def __format__(self, format_spec):
        # IDEA: Maybe titan should support some format_spec options like mytable:qualified
        # print("__format__", self.__class__, format_spec, self.name, self.graph is None)
        # print("^^^^^", inspect.currentframe().f_back.f_back.f_code.entity_cls)

        if self.graph and format_spec != "raw":
            self.graph.entity_referenced(self)

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

    def depends_on(self, *entities):
        for entity in entities:
            if not isinstance(entity, Entity):
                raise Exception(f"[{entity}:{type(entity)}] is not an Entity")
            self.dependencies.append(entity)

    def fully_qualified_name(self):
        raise NotImplementedError


class AccountLevelEntity(Entity):
    @classmethod
    def sql(cls, query_text: str):
        _, _, name = parse_names(query_text)
        return cls(name=name, query_text=query_text)

    def fully_qualified_name(self):
        return self.name.upper()


class DatabaseLevelEntity(Entity):
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


class SchemaLevelEntity(Entity):
    def __init__(self, database=None, schema=None, **kwargs):
        super().__init__(**kwargs)
        self.database = database
        self.schema = schema

    @classmethod
    def sql(cls, query_text: str, **kwargs):
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


class Database(AccountLevelEntity):
    def __init__(self, name, query_text=None, implicit=False):
        query_text = query_text or f"CREATE DATABASE {name.upper()}"
        super().__init__(name=name, query_text=query_text)

    def schema(self, schemaname):
        # table = Table(name=tablename, database=self, schema=self.implicit_schema, implicit=True)
        if schemaname != "PUBLIC":
            raise NotImplementedError
        public_schema = Schema(name=schemaname, database=self, implicit=True)

        # TODO: there needs to be a way for share to bring its ridealongs
        if self.graph:
            self.graph.add(public_schema)

        return public_schema

    def fully_qualified_name(self):
        name = self.name.upper()
        return name


class Schema(DatabaseLevelEntity):
    pass


class Table(SchemaLevelEntity):
    # PROPS = """
    #   [ CLUSTER BY ( <expr> [ , <expr> , ... ] ) ]
    #   [ STAGE_FILE_FORMAT = ( { FORMAT_NAME = '<file_format_name>'
    #                            | TYPE = { CSV | JSON | AVRO | ORC | PARQUET | XML } [ formatTypeOptions ] } ) ]
    #   [ STAGE_COPY_OPTIONS = ( copyOptions ) ]
    #   [ DATA_RETENTION_TIME_IN_DAYS = <integer> ]
    #   [ MAX_DATA_EXTENSION_TIME_IN_DAYS = <integer> ]
    #   [ CHANGE_TRACKING = { TRUE | FALSE } ]
    #   [ DEFAULT_DDL_COLLATION = '<collation_specification>' ]
    #   [ COPY GRANTS ]
    #   [ [ WITH ] ROW ACCESS POLICY <policy_name> ON ( <col_name> [ , <col_name> ... ] ) ]
    #   [ [ WITH ] TAG ( <tag_name> = '<tag_value>' [ , <tag_name> = '<tag_value>' , ... ] ) ]
    #   [ COMMENT = '<string_literal>' ]
    # """

    PROPS = PROPS["Table"]

    def __init__(self, *args, autoload=False, **kwargs):
        super().__init__(*args, **kwargs)

        # TODO: make this a changeable property that registers/deregisters the pipe when the flag is flipped
        self.autoload = autoload
        self.table_stage = Stage(name=f"@%{self.name}", implicit=True)
        self.table_stage.depends_on(self)

    # def create(self, session):
    #     super().create(session)
    #     if self.autoload:
    #         raise NotImplementedError
    # Needs a refactor via dependencies
    # # Does this need to be a pipe we refresh, or should we just call the COPY INTO command each time?
    # pipe = Pipe(
    #     sql=rf"""
    #     CREATE PIPE {self.name}_autoload_pipe
    #         AS
    #         COPY INTO {self.name}
    #         FROM {self.table_stage}
    #         FILE_FORMAT = (
    #             TYPE = CSV
    #             SKIP_HEADER = 1
    #             COMPRESSION = GZIP
    #             FIELD_OPTIONALLY_ENCLOSED_BY = '\042'
    #             NULL_IF = '\N'
    #             NULL_IF = 'NULL'
    #         )
    #     """,
    # )
    # pipe.create(session)


class View(SchemaLevelEntity):
    pass


class Sproc(SchemaLevelEntity):
    pass
    # @classmethod
    # def func(cls, func_):
    #     # database, schema, name = parse_names(query_text)
    #     return cls(name=name, query_text=query_text)


class Stage(SchemaLevelEntity):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.hooks = {"on_file_added": None}

    @property
    def on_file_added(self):
        return self.hooks["on_file_added"]

    @on_file_added.setter
    def on_file_added(self, hook):
        # TODO: This needs to be refactored to be wrapped in a Sproc entity and for dependencies to be implicitly tracked
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


class Function(SchemaLevelEntity):
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


class Pipe(SchemaLevelEntity):
    pass


class FileFormat(SchemaLevelEntity):
    pass


class Share(AccountLevelEntity):
    """
    CREATE DATABASE
        IDENTIFIER('SNOWPARK_FOR_PYTHON__HANDSONLAB__WEATHER_DATA')
    FROM SHARE
        IDENTIFIER('WEATHERSOURCE.SNOWFLAKE_MANAGED$PUBLIC_GCP_US_CENTRAL1."WEATHERSOURCE_SNOWFLAKE_SNOWPARK_TILE_SNOWFLAKE_SECURE_SHARE_1651768630709"');
    """

    def __init__(self, listing, name, accept_terms=False):
        super().__init__(name=name)
        self.listing = listing
        self.accept_terms = accept_terms
        self.database_share = 'WEATHERSOURCE.SNOWFLAKE_MANAGED$PUBLIC_GCP_US_CENTRAL1."WEATHERSOURCE_SNOWFLAKE_SNOWPARK_TILE_SNOWFLAKE_SECURE_SHARE_1651768630709"'
        self.implicit_schema = Schema(name="ONPOINT_ID", database=self, implicit=True)

        # SHOW OBJECTS IN DATABASE WEATHER_NYC

    def create(self, session):
        # Punting for now. Not sure if this is better represented as a dependency in the entity graph
        if self.accept_terms:
            session.sql(f"CALL SYSTEM$ACCEPT_LEGAL_TERMS('DATA_EXCHANGE_LISTING', '{self.listing}');").collect()
        session.sql(
            f"""
            CREATE DATABASE {self.name}
            FROM SHARE {self.database_share}
            """
        ).collect()

    def table(self, tablename):
        table = Table(name=tablename, database=self, schema=self.implicit_schema, implicit=True)

        # TODO: there needs to be a way for share to bring its ridealongs
        if self.graph:
            self.graph.add(table)

        return table

    @classmethod
    def show(cls, session):
        return [row.listing_global_name for row in session.sql("SHOW SHARES").collect()]


class EntityPointer(Entity):
    @classmethod
    def show(cls, session):
        return []

    def create(self, session):
        return

    # def fully_qualified_name(self):
    #     return self.name


class NullEntity(Entity):
    def __init__(self):
        pass


class EntityGraph:
    """
    The EntityGraph is a DAG that manages dependent relationships between Titan entities.

    For example: a table, like most entities, must have a schema and database. The entity graph
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

    ROOT = NullEntity()

    def __init__(self):
        self._graph = {}
        self._in_degree = {}
        self._ref_listener = None
        # self.pending_refs = []

    def __len__(self):
        return len(self._graph.keys())

    @property
    def all(self) -> t.List[Entity]:
        return list(self._graph.keys())

        # While func is executed, the __format__ function for one or more entities may be called
        # we need to find a way to bubble that up so that this View entity is dependent on those
        # other entities

    def add(self, *entities: Entity):
        for entity in entities:
            if entity in self._graph:
                print("Graph > ~", entity.__repr__())
                return
            else:
                print("Graph > +", entity.__repr__())
                # self.root.add(entity)
                self._graph[entity] = set()
                self._in_degree[entity] = 0
                entity.graph = self

                if self._ref_listener:
                    for ref in self._ref_listener:
                        self.add_dependency(entity, ref)

                for dep in entity.dependencies:
                    self.add_dependency(entity, dep)

    def add_dependency(self, entity, dependency):
        # [entity] -needs-> [dependency]

        # I'm trying to figure out if EntityPointers make sense in this system, and if there
        # should exist a way where they start as pointers but get resolved into objects.
        # This is common for DBs and schemas that might live in existing code

        self.add(dependency)
        if dependency not in self._graph[entity]:
            self._graph[entity].add(dependency)
            self._in_degree[dependency] += 1

    def notify(self, entity):
        """
        An entity has just notified us that it is being interpolated. Add it to a list of items to
        tack on as dependencies
        """
        # self.pending_refs.append(entity)
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

    def entity_referenced(self, entity):
        print(">&>&> entity referenced")
        if self._ref_listener is not None:
            print(">&>&> entity ref added")
            self._ref_listener.add(entity)


# NOTE: Catalog should probably crawl a share and add all its tables and views into the catalog
class EntityCatalog:
    def __init__(self, session):
        self._catalog = {}
        self._session = session

    def __contains__(self, entity):
        entity_cls = type(entity)
        if entity_cls not in self._catalog:
            self._catalog[entity_cls] = entity_cls.show(self._session)
        entity_identifier = entity.name
        if entity_cls is Share:
            entity_identifier = entity.listing
        return entity_identifier in self._catalog[entity_cls]
