import inspect

import sqlglot

# import snowflake.snowpark.functions as sf
# from snowflake.snowpark.stored_procedure import StoredProcedureRegistration

from titan.client import get_session
from titan import ent

# import snowflake.connector


class App:
    def __init__(self, database, schema):
        self._session = get_session()
        self._entities = ent.EntityGraph()

        self._database = ent.Database(name=database)
        self._schema = ent.Schema(name=schema, database=self._database)
        self.entities.add(self._database, self._schema)

    @property
    def entities(self):
        return self._entities

    # @property
    # def database(self):
    #     return self._database.name

    # @property
    # def schema(self):
    #     return self._schema.name

    @property
    def session(self):
        return self._session

    def create(self):
        # TODO: I need to do catalog building hand-in-hand with this
        self.session.query_tag = "titan:run::0xD34DB33F"
        self.session.sql("SELECT '[Titan run=0xD34DB33F] begin'").collect()

        processed = set()

        catalog = ent.EntityCatalog(self.session)
        if self._database not in catalog:
            self._database.create(self.session)
            processed.add(self._database)
        self.session.use_database(self._database.name)

        # # One day this should be abstracted in a way that supports multiple sessions, with a nice context manager
        # # so I can write code like
        # # with app.in_schema("ZIPZAP") as app:
        if self._schema not in catalog:
            self._schema.create(self.session)
            processed.add(self._schema)
        self.session.use_schema(self._schema.name)

        # NOTE: How should namespace conflicts work? Should this be an app-level config, something like strict=True
        # means that app will only ever create and error on exisiting.
        # I think terraform does this by doing a state refresh first (the things it knows about) and then
        # just diffs plan with state
        # So that path requires an import in advance. I feel like that's a little too strict.
        # I think I want the default behavior to be just "create if not exists" for most things
        # For shares I will have to special case because they are global

        # For this to work, I think entities need to be limited to only creating themselves, and adding
        # other entities via dependencies

        # BUGFIX: There's an issue where I have generated 2 entities with the same name

        # print("*CATALOG*", catalog)
        for entity in self.entities.all:
            # Some entities are front-loaded, so skip any that have been processed
            if entity in processed:
                continue

            print(">>>>> Creating", type(entity).__name__, entity.name, flush=True)
            if entity not in catalog:
                entity.create(self._session)
            else:
                print("... skipped")
            processed.add(entity)
        self.session.sql("SELECT '[Titan run=0xD34DB33F] end'").collect()

    # def _create(self, entity, session):
    #     entity.create(session)

    def stage(self):
        def inner(func):
            def wrapper(*args, **kwargs):
                res = func(*args, **kwargs)
                stage = ent.Stage(res)
                self.entities.add(stage)
                return stage

            return wrapper

        return inner

    def table(self):
        def inner(func):
            def wrapper(*args, **kwargs):
                res = func(*args, **kwargs)
                table = ent.Table(res)
                self.entities.add(table)
                return table

            # wrapper.entity_cls = ent.Table

            return wrapper

        return inner

    def view(self):
        def inner(func):
            def wrapper(*args, **kwargs):
                res = func(*args, **kwargs)
                view = ent.View(res)
                self.entities.add(view)
                return view

            return wrapper

        return inner

    def sproc(self):
        def inner(func):
            def wrapper(*args, **kwargs):
                sp = ent.Sproc(func)
                self.entities.add(sp)
                return sp

            return wrapper

        return inner

    # TODO: There should be a unified interface where I can initialize any app
    # entity using a decorator or a factory function
    # Since shares dont have a well definied SQL interface, only factory makes sense
    def share(self, *args, **kwargs):
        _share = ent.Share(*args, **kwargs)
        self.entities.add(_share)
        return _share

    def entrypoint(self):
        def inner(func):
            def wrapper(*args, **kwargs):
                func(*args, **kwargs)
                return self

            return wrapper

        return inner
