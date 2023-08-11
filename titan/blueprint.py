from dictdiffer import diff

from .data_provider import DataProvider
from .enums import Scope
from .identifiers import URN
from .resources import RoleGrant


def remove_none_values(d):
    return {k: v for k, v in d.items() if v is not None}


def fetch_remote_state(provider: DataProvider, manifest):
    state = {}
    for urn_str in manifest.keys():
        urn = URN.from_str(urn_str)
        data = provider.fetch_resource(urn)
        if data:
            state[urn_str] = remove_none_values(data)

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
        # refs = []
        for resource in self.staged:
            if resource.implicit:
                continue
            self._finalize_scope(resource)
            urn = URN.from_resource(account=self.account, resource=resource)
            data = resource.model_dump(exclude_none=True)  # mode="json",

            manifest_key = str(urn)
            if isinstance(resource, RoleGrant):
                # TODO: codesmell
                if manifest_key not in manifest:
                    manifest[manifest_key] = {"to_role": [], "to_user": []}
                if "to_role" in data:
                    manifest[manifest_key]["to_role"].append(data)
                else:
                    manifest[manifest_key]["to_user"].append(data)
            else:
                if manifest_key in manifest:
                    raise RuntimeError(f"Duplicate resource found {manifest_key}")
                manifest[manifest_key] = data
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
        return Plan(remote_state, manifest)

    def apply(self, session, plan=None):
        plan = plan or self.plan(session)

        provider = DataProvider(session)
        for action, urn_str, data in plan.changes:
            urn = URN.from_str(urn_str)
            if action == "create":
                provider.create_resource(urn, data)
            elif action == "update":
                provider.update_resource(urn, data)
            elif action == "delete":
                raise NotImplementedError
            else:
                raise Exception(f"Unexpected action {action} in plan")

    def destroy(self, session, manifest=None):
        manifest = manifest or self.generate_manifest()

        provider = DataProvider(session)
        for urn_str, data in manifest.items():
            urn = URN.from_str(urn_str)
            provider.drop_resource(urn, data)

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
        diffs = list(diff(remote_state, manifest))
        print("~" * 120)
        print_diffs(diffs)
        print("~" * 120)
        for action, target, deltas in diffs:
            if action == "add":
                for delta in deltas:
                    urn_str = delta[0]
                    self.changes.append(("create", urn_str, manifest[urn_str]))
            elif action == "change":
                urn_str, modified_attr = target[0], target[1]
                new_value = deltas[-1]
                self.changes.append(("update", urn_str, {modified_attr: new_value}))
