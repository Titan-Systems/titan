from dictdiffer import diff


from .resource import Scope
from .resources.database import Database
from .resources.view import View


class Blueprint:
    def __init__(self, name=None) -> None:
        self.name = name
        self.staged = []

    def plan(self, session):
        # 1. Build a graph from staged dependencies
        adapter = Adapter(session)
        # account = Account(name=adapter.account, region=adapter.region)
        # self.add(adapter.account)

        config = {}
        state = {}
        for resource in self.staged:
            if resource.urn in config:
                raise Exception(f"Duplicate resource, {resource.urn}")
            if resource.scope == Scope.ACCOUNT:
                resource.account = adapter.account
            resource.finalize()
            if resource.name == "TPCH_SF10_ORDERS":
                print(resource)
            config[resource.urn] = resource.model_dump(mode="json", by_alias=True)
        for urn, res in config.items():
            print("<<<<<", urn, ">>>>>\n", res)

        for resource in self.staged:
            if isinstance(resource, Database):
                state[resource.urn] = adapter.fetch_database(resource.urn)
            elif isinstance(resource, View):
                state[resource.urn] = adapter.fetch_view(resource.urn)

        d = diff(state, config)
        print("~" * 120)
        for action, target, deltas in d:
            print(f"[{action}]", target)
            for delta in deltas:
                print("\t", delta)
        print("ok")
        # print(graph)

    def apply(self, session):
        pass

    def _add(self, resource):
        self.staged.append(resource)

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


class Provisioner:
    pass
