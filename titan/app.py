from __future__ import annotations

# import inspect
import re

from typing import Optional, Dict, Union

import sqlglot

from sqlglot import exp

# import snowflake.snowpark.functions as sf
# from snowflake.snowpark.stored_procedure import StoredProcedureRegistration

# from .props import Identifier
from .resource import Resource, AccountLevelResource, DatabaseLevelResource, SchemaLevelResource
from .resource_graph import ResourceGraph

from .account import Account
from .catalog import Catalog
from .database import Database
from .dynamic_table import DynamicTable
from .file_format import FileFormat
from .grants import RoleGrant, PrivGrant
from .pipe import Pipe
from .resource_monitor import ResourceMonitor
from .role import Role
from .schema import Schema
from .stage import Stage
from .table import Table
from .task import Task
from .user import User
from .warehouse import Warehouse


from .policy import Policy, PolicyPack


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
        policy: Optional[Union[Policy, PolicyPack]] = None,
    ):
        # self._session = get_session()
        self._resources = ResourceGraph()
        self.account = account if isinstance(account, Account) else Account(name=account)
        self.resources.add(self.account)

        database_ = Database.all[database]
        if database_:
            self.resources.add(database_)

        if schema is not None:
            if database_ is None:
                # TODO: infer database from connection and config
                raise Exception("Cant have schema without database")
            schema_ = Schema.all[schema]
            schema_.database = database_
            self.resources.add(schema_)

        self.policy = policy

        self._entrypoint = None
        self._auto_register = False
        # Resource.on_init = self.resources.add

    @property
    def resources(self):
        return self._resources

    @property
    def session(self):
        return self._session

    # def build(self):
    #     if self._entrypoint is None:
    #         raise Exception("No app entrypoint is defined")
    #     self._auto_register = True
    #     self._entrypoint()
    #     self._auto_register = False

    def check_policies(self):
        if self.policy is None:
            return

        violations = []

        def report_violation(violation: str):
            violations.append(violation)

        if isinstance(self.policy, Policy):
            policies = [self.policy]
        elif isinstance(self.policy, PolicyPack):
            policies = self.policy.policies

        for policy in policies:
            for resource in self.resources.all:
                if (
                    isinstance(
                        resource,
                        (
                            User,
                            Role,
                        ),
                    )
                    and not resource.stub
                    and not resource.implicit
                ):
                    policy.validate(resource, report_violation)
        return violations

    def build(self):
        """
        Resolve stub references
        """
        for resource in self.resources.all:
            resource.finalize()
            print(repr(resource), "\n\t->", resource.requirements)
        policy_violations = self.check_policies()
        if policy_violations:
            for pv in policy_violations:
                print(pv)

    def run(self):
        # self.build()

        # print(self.resources.sorted())
        # return

        # TODO: I need to do catalog building hand-in-hand with this
        self.session.query_tag = "titan:run::0xD34DB33F"
        self.session.sql("SELECT '[Titan run=0xD34DB33F] begin'").collect()

        processed = set()

        catalog = Catalog(self.session)
        # if self.database not in catalog:
        #     self.database.create(self.session)
        #     processed.add(self.database)
        # self.session.use_database(self.database.name)

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

    def parse_sql(self, sql_blob: str) -> None:
        # stmts = sqlglot.parse(sql_blob, read="snowflake")
        stmts = sql_blob.split(";")
        # I tried T_Resource here but there's some issue with binding that I dont understand
        # Dict[str, Optional[Resource]]
        local_state: Dict[str, Optional[Resource]] = {
            "active_role": None,
            "active_account": self.account,
            "active_database": None,
            "active_schema": None,
        }

        extract_create_kind = re.compile(
            r"""
            CREATE\s+
            (?:OR\s+REPLACE\s+)?
            (?:TRANSIENT\s+)?
            (?:TEMPORARY\s+)?
            (?:TEMP\s+)?
            (?P<create_kind>(
                DATABASE |
                DYNAMIC\s+TABLE |
                FILE\s+FORMAT |
                PIPE |
                RESOURCE\s+MONITOR |
                ROLE |
                STAGE |
                TABLE |
                TASK |
                USER |
                WAREHOUSE |
            ))
        """,
            re.VERBOSE | re.IGNORECASE,
        )

        for i, stmt in enumerate(stmts):
            # if stmt is None:
            #     continue
            # sql = stmt.sql(dialect="snowflake")
            sql = stmt.strip()
            new_resource: Optional[Resource] = None

            if isinstance(stmt, exp.Create):
                create_kind = stmt.args["kind"].lower()
                if create_kind == "database":
                    new_resource = Database.from_sql(sql)
                    local_state["active_database"] = new_resource
                    local_state["active_schema"] = new_resource.schemas["PUBLIC"]
                elif create_kind == "table":
                    new_resource = Table.from_sql(sql)
                elif create_kind == "schema":
                    new_resource = Schema.from_sql(sql)
                    local_state["active_schema"] = new_resource
                    if new_resource.database is None:
                        if local_state["active_database"]:
                            new_resource.database = local_state["active_database"]
                        else:
                            raise Exception("Schema specified without database")
            elif isinstance(stmt, exp.Command) and stmt.this.lower() == "create":
                create_kind = extract_create_kind.search(sql).groupdict()["create_kind"].lower()

                if create_kind == "":
                    raise Exception(f"Create kind not found {sql}")

                if create_kind == "warehouse":
                    new_resource = Warehouse.from_sql(sql)
                elif create_kind == "stage":
                    new_resource = Stage.from_sql(sql)
                elif create_kind == "role":
                    new_resource = Role.from_sql(sql)
                elif create_kind == "user":
                    new_resource = User.from_sql(sql)
                elif create_kind == "resource monitor":
                    new_resource = ResourceMonitor.from_sql(sql)
                elif create_kind == "file format":
                    new_resource = FileFormat.from_sql(sql)
                elif create_kind == "pipe":
                    new_resource = Pipe.from_sql(sql)
                elif create_kind == "dynamic table":
                    new_resource = DynamicTable.from_sql(sql)
                elif create_kind == "task":
                    new_resource = Task.from_sql(sql)
                else:
                    raise Exception(f"Unknown create kind {create_kind}")
            elif isinstance(stmt, exp.Command) and stmt.this.lower() == "grant":
                grant_tokens = stmt.expression.this.strip().split()
                grant_kind = grant_tokens[0].lower()
                if grant_kind == "role":
                    new_resource = RoleGrant.from_sql(sql)
                elif grant_kind == "ownership":
                    # GRANT OWNERSHIP
                    pass
                elif grant_kind == "database" and grant_tokens[1].lower() == "role":
                    # GRANT DATABASE ROLE
                    pass
                else:
                    new_resource = PrivGrant.from_sql(sql)
            elif isinstance(stmt, exp.Use):
                use_kind = stmt.args["kind"].this.lower()
                name = stmt.this.this.this
                # USE ROLE SECURITYADMIN;
                if use_kind == "role":
                    new_resource = Role.all[name]
                    local_state["active_role"] = new_resource
                elif use_kind == "database":
                    new_resource = Database.all[name]
                    local_state["active_database"] = new_resource
                # elif use_kind == "schema":
                # local_state["active_schema"] = Schema.all[stmt.this.this.this]

            if new_resource:
                if new_resource.ownable and local_state["active_role"]:
                    new_resource.owner = local_state["active_role"]
                if isinstance(new_resource, AccountLevelResource) and local_state["active_account"]:
                    new_resource.account = local_state["active_account"]
                elif isinstance(new_resource, DatabaseLevelResource) and local_state["active_database"]:
                    new_resource.database = local_state["active_database"]
                elif isinstance(new_resource, SchemaLevelResource) and local_state["active_schema"]:
                    new_resource.schema = local_state["active_schema"]
                # print(">>>>> Creating", type(new_resource).__name__, new_resource.name, flush=True)
                self.resources.add(new_resource)
            else:
                print(repr(stmt))
                print(sql)
                print("...")
                # raise Exception

    def tree(self):
        # raise Exception("This is broken")
        from treelib import Tree

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

    # def stage(self):
    #     def inner(func):
    #         def wrapper(*args, **kwargs):
    #             res = func(*args, **kwargs)
    #             if type(res) is str:
    #                 stage = Stage.from_sql(res)
    #                 self.resources.add(stage)
    #                 return stage
    #             else:
    #                 raise NotImplementedError

    #         return wrapper

    #     return inner

    # def table(self, **table_kwargs):
    #     def inner(func):
    #         def wrapper(*args, **kwargs):
    #             res = func(*args, **kwargs)
    #             if type(res) is str:
    #                 table = Table.from_sql(res, **table_kwargs)
    #                 self.resources.add(table)
    #                 return table
    #             else:
    #                 raise NotImplementedError

    #         return wrapper

    #     return inner

    # def view(self):
    #     def inner(func):
    #         def wrapper(*args, **kwargs):
    #             with self._resources.capture_refs() as ctx:
    #                 res = func(*args, **kwargs)
    #                 view = View.from_sql(res)
    #                 ctx.add(view)
    #                 return view

    #         return wrapper

    #     return inner

    # def sproc(self):
    #     def inner(func):
    #         def wrapper(*args, **kwargs):
    #             with self._resources.capture_refs() as ctx:
    #                 res = func(*args, **kwargs)
    #                 if type(res) is str:
    #                     sproc = Sproc.from_sql(res)
    #                 # elif callable(res):
    #                 #     sproc = Sproc.func(res)
    #                 else:
    #                     raise NotImplementedError
    #                 ctx.add(sproc)
    #                 return sproc

    #         return wrapper

    #     return inner

    # # TODO: There should be a unified interface where I can initialize any app
    # # resource using a decorator or a factory function
    # # Since shares dont have a well definied SQL interface, only factory makes sense
    # def share(self, *args, **kwargs):
    #     _share = Share(*args, **kwargs)
    #     self.resources.add(_share)
    #     return _share

    # def entrypoint(self, tags: List[str] = []):
    #     def inner(func):
    #         self._entrypoint = func
    #         return func

    #     return inner
