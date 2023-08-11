from dictdiffer import diff

from .data_provider import DataProvider
from .enums import Scope
from .identifiers import URN


def fetch_remote_state(provider: DataProvider, manifest):
    state = {}
    for urn_str in manifest.keys():
        urn = URN.from_str(urn_str)
        resource = provider.fetch_resource(urn)
        if resource:
            state[urn_str] = resource

    return state


def print_diffs(diffs):
    for action, target, deltas in diffs:
        print(f"[{action}]", target)
        for delta in deltas:
            print("\t", delta)


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
            value = resource.model_dump(exclude_none=True)  # mode="json",
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

    def plan(self, session):
        provider = DataProvider(session)
        manifest = self.generate_manifest()
        remote_state = fetch_remote_state(provider, manifest)

        # diffs = diff(remote_state, manifest)
        # print_diffs(diffs)
        return Plan(remote_state, manifest)

    def deploy(self, session, plan=None):
        plan = plan or self.plan(session)

        provider = DataProvider(session)
        for action, urn_str, data in plan.changes:
            urn = URN.from_str(urn_str)
            if action == "create":
                # resource.create(session)
                pass
            elif action == "update":
                # resource.update(session)
                provider.update_resource(urn, data)
            # elif action == "delete":
            #     resource.delete(session)
            else:
                raise Exception(f"Unexpected action {action} in plan")

    def _add(self, resource):
        self.staged.append(resource)

    def add(self, *resources):
        if isinstance(resources[0], list):
            resources = resources[0]
        for resource in resources:
            self._add(resource)


class Plan:
    def __init__(self, remote_state, manifest):
        self.changes = []
        diffs = diff(remote_state, manifest)
        # print_diffs(diffs)
        for action, target, deltas in diffs:
            # urn_str = target[0]
            # urn = URN.from_str(urn_str)
            if action == "add":
                for delta in deltas:
                    urn_str = delta[0]
                    self.changes.append(("create", urn_str, manifest[urn_str]))
            elif action == "change":
                # modified_attr = target[-1]
                #
                urn_str, modified_attr = target[0], target[1]
                new_value = deltas[-1]
                self.changes.append(("update", urn_str, {modified_attr: new_value}))

        # for change in self.changes:
        #     print(change)
