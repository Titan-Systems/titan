from dictdiffer import diff
from .enums import Scope
from .identifiers import URN


def fetch_remote_state(adapter, manifest):
    for urn_str in manifest.keys():
        urn = URN.from_str(urn_str)
        adapter.fetch_resource(urn)


class Blueprint:
    def __init__(self, name, account, database=None, schema=None, resources=[]) -> None:
        self.staged = []
        self.add(resources or [])
        # NOTE: might want a resolution function here
        self.account = account
        self.database = database
        self.schema = schema

    def generate_manifest(self):
        manifest = {}
        refs = []
        for resource in self.staged:
            if resource.implicit:
                continue
            self._finalize_scope(resource)
            key = URN.from_resource(account=self.account, resource=resource)
            value = resource.model_dump(exclude_none=True)
            manifest[str(key)] = value
            # for ref in resource.refs:
            #     refs.append([key, urn(self.account, ref)])
        return manifest

    def _finalize_scope(self, resource):
        # TODO: connect stubs
        if resource.parent is None:
            if resource.scope == Scope.ACCOUNT:
                resource.account = self.account
            elif resource.scope == Scope.DATABASE:
                if self.database is None:
                    raise Exception(f"Orphaned resource found {resource}")
                resource.database = self.database
            elif resource.scope == Scope.SCHEMA:
                if self.schema is None:
                    raise Exception(f"Orphaned resource found {resource}")
                resource.schema = self.schema

    def plan(self, adapter):
        manifest = self.generate_manifest()
        remote_state = fetch_remote_state(adapter, manifest)
        diffs = diff(manifest, remote_state)
        for action, target, deltas in diffs:
            print(f"[{action}]", target)
            for delta in deltas:
                print("\t", delta)
        print("ok")

    # def compare(self, remote_state):
    #     return diff(self.manifest, remote_state)

    # d = diff(state, config)
    # print("~" * 120)
    # for action, target, deltas in d:
    #     print(f"[{action}]", target)
    #     for delta in deltas:
    #         print("\t", delta)

    # def apply(self):
    #     # diff = self.plan()
    #     pass

    def _add(self, resource):
        self.staged.append(resource)

    def add(self, *resources):
        if isinstance(resources[0], list):
            resources = resources[0]
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

"""


class Provisioner:
    pass
