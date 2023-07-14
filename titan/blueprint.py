# from snowflake.snowpark import Session

from .urn import URN

from dataclasses import dataclass


# @dataclass
# class Database:
#     urn: URN
#     name: str
#     data_retention_time_in_days: int
#     comment: str
#     owner: str


class Adapter:
    def __init__(self, session):
        self.session = session
        self.account = self.fetch_account()
        self.region = self.fetch_region()

    def fetch_account(self):
        with self.session.cursor() as cur:
            account = cur.execute("SELECT CURRENT_ACCOUNT()").fetchone()[0]
        return account

    def fetch_region(self):
        with self.session.cursor() as cur:
            account = cur.execute("SELECT CURRENT_REGION()").fetchone()[0]
        return account

    def fetch_databases(self):
        print(self.account, self.region)
        # with self.session.cursor() as cur:
        #     databases = cur.execute("SHOW DATABASES").fetchall()
        #     for (
        #         created_on,
        #         name,
        #         is_default,
        #         is_current,
        #         origin,
        #         owner,
        #         comment,
        #         options,
        #         retention_time,
        #         kind,
        #     ) in databases:
        #         if kind == "STANDARD":
        #             db = Database(
        #                 urn=URN(self.region, self.account, "database", name),
        #                 name=name,
        #                 data_retention_time_in_days=retention_time,
        #                 comment=comment,
        #                 owner=owner,
        #             )
        #         elif kind == "IMPORTED DATABASE":
        #             sh = Share()
        # print("ok")
        # return databases


class Blueprint:
    def __init__(self, name=None) -> None:
        self.name = name
        self.staged = []
        self.staged_types = set()

    def plan(self, session):
        # 1. Build a graph from staged dependencies
        # 2. Check graph for ciruclar dependencies
        sf = Adapter(session)
        # databases = sf.fetch_databases()

    def apply(self, session):
        pass

    def fetch_state(self, session):
        with session.cursor() as cur:
            databases = cur.execute("SHOW DATABASES").fetchall()
        print(databases)

    def _add(self, resource):
        self.staged.append(resource)
        self.staged_types.add(type(resource))

    def add(self, *resources):
        for resource in resources:
            self._add(resource)


"""

@titan.blueprint
def prod():
    admin_db = titan.Database("ADMIN")
    shared_db = titan.Database("SHARED")
    # setup_base(base_db)
    provisioner = titan.Role("provisioner")
    provisioner.grant(client_database.required_permissions)
    return {}

@titan.blueprint(policies=[titan.Policy(...), titan_standard_policy])
def prod():
    provisioner = titan.Role("provisioner")
    provisioner.grant(client_database.required_permissions)
    return (
        titan.Database("ADMIN"),
        titan.Database("SHARED"),
        provisioner,
    )

Ideas
 - blueprints have parameters to let them be stamped out
 - blueprints have exports (why?)

Stuck
 - How can we write a blueprint that doesnt result in a bunch of hanging python objects?
   Explicit is probably better than implicit



"""
