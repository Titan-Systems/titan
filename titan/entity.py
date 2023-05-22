import inspect
import typing as t

from collections import defaultdict

from titan.parser import parse_name
from titan.hooks import on_file_added_factory


class Entity:
    # __slots__ = ("sql", "name", "state", "_dependencies")
    def __init__(self, sql=None, name=None):
        self.sql = sql
        if name is not None:
            self.name = name
        elif sql is not None:
            self.name = parse_name(sql)
        else:
            pass  # probably error state

        self.graph = None
        self.state = {}
        self._dependencies = []

    def __format__(self, format_spec):
        # IDEA: Maybe titan should support some format_spec options like mytable:qualified
        print("__format__", self.__class__, format_spec, self.name, self.graph is None)
        # print("^^^^^", inspect.currentframe().f_back.f_back.f_code.entity_cls)
        if self.graph:
            self.graph.notify(self)

        return self.name.upper()

    @classmethod
    def show(cls, session):
        q = f"SHOW {cls.__name__.upper()}S"
        return [row.name for row in session.sql(q).collect()]

    def __repr__(self):
        return f"{type(self).__name__}:{self.name}"

    def create(self, session):
        print("~" * 8, type(self).__name__, self.name)
        if self.sql:
            session.sql(self.sql).collect(block=False)
        else:
            print("!!!!! Creation Failed, no SQL to run", self.name)

    def depends_on(self, entity_or_str):
        print(f"(I DEPEND ON) {self.__repr__()} => {entity_or_str.__repr__()}")
        if type(entity_or_str) is Entity:
            entity = entity_or_str
        else:
            entity = EntityPointer(name=entity_or_str)

        self._dependencies.append(entity)

    @property
    def dependencies(self):
        try:
            return self._dependencies
        except:
            return []


class Database(Entity):
    pass
    # @classmethod
    # def show(self, session):
    #     return [row.name for row in session.sql("SHOW DATABASES").collect()]


class Schema(Entity):
    def __init__(self, name, database):
        self.name = name
        self.sql = f"CREATE SCHEMA {database}.{name}"
        self._dependencies = []
        self.depends_on(database)

    # @classmethod
    # def show(self, session):
    #     return [row[1] for row in session.sql("SHOW SCHEMAS").collect()]

    # def __init__(self, sql=None, name=None):
    #     self.sql = sql
    #     if name is not None:
    #         self.name = name
    #     elif sql is not None:
    #         self.name = parse_name(sql)
    #     else:
    #         pass  # probably error state


class Table(Entity):
    def __init__(self, sql):
        super().__init__(sql=sql)
        # TODO: make this a changeable property that registers/deregisters the pipe when the flag is flipped
        self.autoload = False

    def create(self, session):
        super().create(session)
        if self.autoload:
            raise NotImplementedError
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

    @property
    def table_stage(self):
        return f"@%{self.name}"


class View(Entity):
    def __init__(self, sql):
        # TODO: this is codesmell
        super().__init__(sql=sql)


class Sproc(Entity):
    def __init__(self, func):
        self.name = func.__name__
        self.func = func()

        print("sprocname", self.func.__name__)

        sql = "SELECT 'this is a sproc'"
        self.sql = sql
        self.ast = sqlglot.parse(sql, read="snowflake")


class Stage(Entity):
    def __init__(self, sql):
        super().__init__(sql=sql)
        self.hooks = {"on_file_added": None}

    @property
    def on_file_added(self):
        return self.hooks["on_file_added"]

    @on_file_added.setter
    def on_file_added(self, hook):
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


class Function(Entity):
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


class Pipe(Entity):
    pass


class FileFormat(Entity):
    pass


class Share(Entity):
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
        # TODO: Implement me
        # NOTE: You want whatever references this entity to rely on this share
        # Yeah something isnt right because this is creating entities not connected to the graph
        # Maybe that's ok?
        pointer = EntityPointer(name=tablename)  # , depends_on(self)
        pointer.depends_on(self)
        if self.graph:
            self.graph.add(pointer)
        return pointer

    @classmethod
    def show(cls, session):
        return [row.listing_global_name for row in session.sql("SHOW SHARES").collect()]


class EntityPointer(Entity):
    def create(self, session):
        return

    @classmethod
    def show(cls, session):
        return []


class NullEntity(Entity):
    def __init__(self):
        pass


class EntityGraph:
    ROOT = NullEntity()

    def __init__(self):
        self._graph = defaultdict(set)
        self.pending_refs = []

    def __len__(self):
        return len(self.all)

    @property
    def all(self) -> t.List[Entity]:
        return list(self.root)

    @property
    def root(self) -> t.Set:
        return self._graph[self.ROOT]

    @property
    def types(self):
        return [type(ent) for ent in self.root]

    def add(self, *entities: Entity):
        for entity in entities:
            if entity in self.root:
                print("Graph > ~", entity.__repr__())
            else:
                print("Graph > +", entity.__repr__())
                self.root.add(entity)
                entity.graph = self

                if self.pending_refs:
                    print("Graph >", "pending_refs", len(self.pending_refs))
                    entity.depends_on(*self.pending_refs)
                    self.pending_refs = []

                # We want dependencies to be lazily evaluated. We want something like
                # app.sql("CREATE TEEJ.FIZBUZZ") to work without previously specifying
                # TEEJ database

                for dep in entity.dependencies:
                    if type(dep) is EntityPointer:
                        pass
                    else:
                        self._graph[entity].add(dep)
                        # self.add(dep)

    def notify(self, entity):
        """
        An entity has just notified us that it is being interpolated. Add it to a list of items to
        tack on as dependencies
        """
        self.pending_refs.append(entity)


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
