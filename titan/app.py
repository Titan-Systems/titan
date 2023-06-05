from __future__ import annotations

import inspect

from typing import List, Union

import sqlglot

from sqlglot import exp

# import snowflake.snowpark.functions as sf
# from snowflake.snowpark.stored_procedure import StoredProcedureRegistration

from .parser import parse
from .resource import Resource, AccountLevelResource, DatabaseLevelResource, SchemaLevelResource
from .resource_graph import ResourceGraph

from .account import Account
from .catalog import Catalog
from .database import Database, parse_database
from .role import Role
from .schema import Schema, parse_schema
from .share import Share
from .sproc import Sproc
from .stage import Stage
from .table import Table
from .view import View
from .warehouse import Warehouse


class App:
    def __init__(
        self,
        # TODO: Support inferring account from connection string
        account: Union[str, Account] = "uj63311.us-central1.gcp",
        database: Union[None, str, Database] = None,
        schema: Union[None, str, Schema] = None,
        warehouse: Union[None, str, Warehouse] = None,
        # TODO: implement me.  Maybe this is owner role?
        role: Union[None, str, Role] = None,
    ):
        self._session = get_session()
        self._resources = ResourceGraph()
        self.account = account if isinstance(account, Account) else Account(name=account)

        database = parse_database(database)
        if database is not None:
            self.resources.add(database)

        schema = parse_schema(schema)
        if schema is not None:
            schema.database = database
            self.resources.add(schema)

        self._entrypoint = None
        self._auto_register = False
        Resource.on_init = self.resources.add

    @property
    def resources(self):
        return self._resources

    @property
    def session(self):
        return self._session

    def build(self):
        if self._entrypoint is None:
            raise Exception("No app entrypoint is defined")
        self._auto_register = True
        self._entrypoint()
        self._auto_register = False

    def run(self):
        # self.build()

        # print(self.resources.sorted())
        # return

        # TODO: I need to do catalog building hand-in-hand with this
        self.session.query_tag = "titan:run::0xD34DB33F"
        self.session.sql("SELECT '[Titan run=0xD34DB33F] begin'").collect()

        processed = set()

        catalog = Catalog(self.session)
        if self.database not in catalog:
            self.database.create(self.session)
            processed.add(self.database)
        self.session.use_database(self.database.name)

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
        # print("resource_list_sorted", resource_list_sorted)

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
        stmts = sqlglot.parse(sql_blob, read="snowflake")
        local_state = {}
        for i, stmt in enumerate(stmts):
            # print(repr(stmt))
            if isinstance(stmt, exp.Create):
                create_kind = stmt.args["kind"].lower()
                if create_kind == "database":
                    self.resources.add(Database.from_expression(stmt))
                elif create_kind == "table":
                    self.resources.add(Table.from_expression(stmt))
            elif isinstance(stmt, exp.Command) and stmt.this.lower() == "create":
                create_kind = stmt.args["expression"].strip().split(" ")[0].lower()
                if create_kind == "warehouse":
                    self.resources.add(Warehouse.from_expression(stmt))
                elif create_kind == "stage":
                    self.resources.add(Stage.from_expression(stmt))
                elif create_kind == "role":
                    pass
            elif isinstance(stmt, exp.Command) and stmt.this.lower() == "grant":
                pass

    def tree(self):
        from treelib import Node, Tree

        t = Tree()
        t.create_node("Account " + self.account.name, self.account.name)
        for res in self.resources.all:
            if isinstance(res, AccountLevelResource):
                t.create_node(type(res).__name__ + " " + res.name, repr(res), parent=self.account.name)
        for res in self.resources.all:
            if isinstance(res, DatabaseLevelResource):
                t.create_node(type(res).__name__ + " " + res.name, repr(res), parent=repr(res.database))
        for res in self.resources.all:
            if isinstance(res, SchemaLevelResource):
                t.create_node(type(res).__name__ + " " + res.name, repr(res), parent=repr(res.schema))
        t.show()

    def stage(self):
        def inner(func):
            def wrapper(*args, **kwargs):
                res = func(*args, **kwargs)
                if type(res) is str:
                    stage = Stage.from_sql(res)
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

    def entrypoint(self, tags: List[str] = []):
        def inner(func):
            self._entrypoint = func
            return func

        return inner
