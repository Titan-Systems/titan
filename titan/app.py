import inspect


from typing import List

import sqlglot

# import snowflake.snowpark.functions as sf
# from snowflake.snowpark.stored_procedure import StoredProcedureRegistration

from .resource import Resource, ResourceGraph

from .database import Database
from .schema import Schema

from .table import Table
from .share import Share
from .catalog import Catalog

# import snowflake.connector


class App:
    def __init__(self, account="uj63311.us-central1.gcp", database=None, schema=None, warehouse=None, role=None):
        self.account = account
        # Probably need an Account resource that has implicit roles for accountadmin, sysadmin, etc

        self._session = None  # get_session()
        self._resources = ResourceGraph()

        self._database = Database(name=database)
        if schema:
            self._active_schema = Schema(name=schema, database=self._database)
        else:
            self._active_schema = self._database.schema("PUBLIC")
        self.resources.add(self._database, self._active_schema)

        self._entrypoint = None
        self._auto_register = False
        Resource.on_init = self.resources.add

    @property
    def resources(self):
        return self._resources

    @property
    def session(self):
        return self._session

    def _add_to_active_schema(self, resource):
        resource.database = self._database
        resource.schema = self._active_schema

    def build(self):
        if self._entrypoint is None:
            raise Exception("No app entrypoint is defined")
        self._auto_register = True
        self._entrypoint()
        self._auto_register = False

    def run(self):
        self.build()

        print(self.resources.sorted())
        return

        # TODO: I need to do catalog building hand-in-hand with this
        self.session.query_tag = "titan:run::0xD34DB33F"
        self.session.sql("SELECT '[Titan run=0xD34DB33F] begin'").collect()

        processed = set()

        catalog = Catalog(self.session)
        if self._database not in catalog:
            self._database.create(self.session)
            processed.add(self._database)
        self.session.use_database(self._database.name)

        # # One day this should be abstracted in a way that supports multiple sessions, with a nice context manager
        # # so I can write code like
        # # with app.in_schema("ZIPZAP") as app:

        # FIXME
        # if self._schema not in catalog:
        #     self._schema.create(self.session)
        #     processed.add(self._schema)
        # self.session.use_schema(self._schema.name)

        # NOTE: How should namespace conflicts work? Should this be an app-level config, something like strict=True
        # means that app will only ever create and error on exisiting.
        # I think terraform does this by doing a state refresh first (the things it knows about) and then
        # just diffs plan with state
        # So that path requires an import in advance. I feel like that's a little too strict.
        # I think I want the default behavior to be just "create if not exists" for most things
        # For shares I will have to special case because they are global

        # For this to work, I think resources need to be limited to only creating themselves, and adding
        # other resources via dependencies

        # BUGFIX: There's an issue where I have generated 2 resources with the same name

        # print("*CATALOG*", catalog)
        resource_list_sorted = [res for res in self.resources.sorted() if not res.implicit]
        print("resource_list_sorted", resource_list_sorted)

        deploy = False
        if deploy:
            for resource in resource_list_sorted:
                # Some resources are front-loaded, so skip any that have been processed
                if resource in processed:
                    continue

                # We need to decide which ROLE will be used to create a particular resource.
                # There are two reasons for this:
                #
                # 1. Ownership: the role that creates is the defactor owner. However, some folks
                #      may decide that a role can own things but not create them
                # 2. Some resources can only be created by specific defaults roles like ACCOUNTADMIN

                print(">>>>> Creating", type(resource).__name__, resource.name, flush=True)
                if resource not in catalog:
                    resource.create(self._session)
                else:
                    print("... skipped")
                processed.add(resource)
        self.session.sql("SELECT '[Titan run=0xD34DB33F] end'").collect()

    def from_sql(self, sql_blob):
        stmts = sqlglot.parse(sql_blob)
        local_state = {}
        for i, stmt in enumerate(stmts):
            print(i, type(stmt), "^" * 80)
            print(stmt)

    def stage(self):
        def inner(func):
            def wrapper(*args, **kwargs):
                res = func(*args, **kwargs)
                if type(res) is str:
                    stage = Stage.from_sql(res)
                    self._add_to_active_schema(stage)
                    self.resources.add(stage)
                    return stage
                else:
                    raise NotImplementedError

            return wrapper

        return inner

    def table(self, **table_kwargs):
        def inner(func):
            def wrapper(*args, **kwargs):
                res = func(*args, **kwargs)
                if type(res) is str:
                    table = Table.from_sql(res, **table_kwargs)
                    # Needs to be migrated to resourcegraph probably
                    self._add_to_active_schema(table)
                    self.resources.add(table)
                    return table
                else:
                    raise NotImplementedError

            return wrapper

        return inner

    def view(self):
        def inner(func):
            def wrapper(*args, **kwargs):
                with self._resources.capture_refs() as ctx:
                    res = func(*args, **kwargs)
                    view = View.from_sql(res)
                    ctx.add(view)
                    return view

            return wrapper

        return inner

    def sproc(self):
        def inner(func):
            def wrapper(*args, **kwargs):
                with self._resources.capture_refs() as ctx:
                    res = func(*args, **kwargs)
                    if type(res) is str:
                        sproc = Sproc.from_sql(res)
                    # elif callable(res):
                    #     sproc = Sproc.func(res)
                    else:
                        raise NotImplementedError
                    ctx.add(sproc)
                    return sproc

            return wrapper

        return inner

    # TODO: There should be a unified interface where I can initialize any app
    # resource using a decorator or a factory function
    # Since shares dont have a well definied SQL interface, only factory makes sense
    def share(self, *args, **kwargs):
        _share = Share(*args, **kwargs)
        self.resources.add(_share)
        return _share

    def entrypoint(self, tags: List[str] = None):
        def inner(func):
            self._entrypoint = func
            return func

        return inner
