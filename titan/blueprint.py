from dictdiffer import diff

from .enums import Scope


def urn(blueprint, resource):
    return f"urn:{blueprint.account}:{resource.resource_key}/{resource.fqn}"


class Blueprint:
    def __init__(self, name, account, database=None, schema=None, resources=[]) -> None:
        self.name = name or ""
        self.staged = []
        self.add(resources or [])
        self.account = account
        self.database = database
        self.schema = schema

    @property
    def manifest(self):
        manifest = {}
        for resource in self.staged:
            self.finalize_scope(resource)
            key = urn(self, resource)
            value = resource.model_dump(exclude_none=True)
            manifest[key] = value
        return manifest

    def finalize_scope(self, resource):
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

    def plan(self):
        pass

    def compare(self, remote_state):
        return diff(self.manifest, remote_state)

        # d = diff(state, config)
        # print("~" * 120)
        # for action, target, deltas in d:
        #     print(f"[{action}]", target)
        #     for delta in deltas:
        #         print("\t", delta)

    def apply(self):
        # diff = self.plan()
        pass

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
