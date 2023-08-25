import json

from queue import Queue

from .data_provider import DataProvider, fetch_remote_state
from .enums import Scope
from .identifiers import URN
from .resources import Resource


def print_diffs(diffs):
    for action, target, deltas in diffs:
        print(f"[{action}]", target)
        for delta in deltas:
            print("\t", delta)


def dict_delta(original, new):
    original_keys = set(original.keys())
    new_keys = set(new.keys())

    delta = {}

    for key in original_keys - new_keys:
        delta[key] = None

    for key in original_keys & new_keys:
        if original[key] != new[key]:
            delta[key] = new[key]

    for key in new_keys - original_keys:
        delta[key] = new[key]

    return delta


def diff(original, new):
    original_keys = set(original.keys())
    new_keys = set(new.keys())

    for key in original_keys - new_keys:
        yield "remove", key, original[key]

    for key in new_keys - original_keys:
        yield "add", key, new[key]

    for key in original_keys & new_keys:
        delta = dict_delta(original[key], new[key])
        for attr, value in delta.items():
            yield "change", key, {attr: value}


def _hash(data):
    return hash(json.dumps(data, sort_keys=True))


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
        urns = []
        for resource in self.staged:
            if resource.implicit:
                continue
            self._finalize_scope(resource)
            urn = URN.from_resource(account=self.account, resource=resource)
            urns.append(str(urn))
            if resource.implicit:
                continue
            data = resource.model_dump(exclude_none=True)

            manifest_key = str(urn)
            if manifest_key in manifest and _hash(manifest[manifest_key]) != _hash(data):
                raise RuntimeError(f"Duplicate resource found {manifest_key}")
            manifest[manifest_key] = data
            for ref in resource.refs:
                ref_urn = URN.from_resource(account=self.account, resource=ref)
                refs.append((str(urn), str(ref_urn)))
        manifest["_refs"] = refs
        manifest["_urns"] = urns
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
        return _plan(remote_state, manifest)

    def apply(self, session, plan=None):
        plan = plan or self.plan(session)

        with session.cursor() as cur:
            # cursor setup, including query tag
            for action, urn_str, data in plan:
                urn = URN.from_str(urn_str)
                resource_cls = Resource.classes[urn.resource_key]
                if action == "add":
                    cur.execute(resource_cls.lifecycle_create(urn.fqn, data))
                elif action == "change":
                    cur.execute(resource_cls.lifecycle_update(urn.fqn, data))
                elif action == "remove":
                    cur.execute(resource_cls.lifecycle_delete(urn.fqn))
                else:
                    raise Exception(f"Unexpected action {action} in plan")

    def destroy(self, session, manifest=None):
        manifest = manifest or self.generate_manifest()
        with session.cursor() as cur:
            for urn_str in manifest.keys():
                urn = URN.from_str(urn_str)
                resource_cls = Resource.classes[urn.resource_key]
                cur.execute(resource_cls.lifecycle_delete(urn.fqn))

    def _add(self, resource):
        self.staged.append(resource)
        for ref in resource.refs:
            self._add(ref)

    def add(self, *resources):
        if isinstance(resources[0], list):
            resources = resources[0]
        for resource in resources:
            self._add(resource)


def topological_sort(manifest, urns):
    # Kahn's algorithm

    # Compute in-degree (# of inbound edges) for each node
    in_degrees = {}
    outgoing_edges = {}

    for node in urns:
        in_degrees[node] = 0
        outgoing_edges[node] = set()

    for node, ref in manifest["_refs"]:
        in_degrees[ref] += 1
        outgoing_edges[node].add(ref)

    # Put all nodes with 0 in-degree in a queue
    queue = Queue()
    for node, in_degree in in_degrees.items():
        if in_degree == 0:
            queue.put(node)

    # Create an empty node list
    nodes = []

    while not queue.empty():
        node = queue.get()
        nodes.append(node)

        # For each of node's outgoing edges
        empty_neighbors = set()
        for edge in outgoing_edges[node]:
            in_degrees[edge] -= 1
            if in_degrees[edge] == 0:
                queue.put(edge)
                empty_neighbors.add(edge)

        # Remove edges to empty neighbors
        outgoing_edges[node].difference_update(empty_neighbors)
    nodes.reverse()
    return {value: index for index, value in enumerate(nodes)}


def _plan(remote_state, manifest):
    manifest = manifest.copy()
    urns = manifest.pop("_urns") + list(remote_state.keys())
    sort_order = topological_sort(manifest, urns)
    del manifest["_refs"]
    # del manifest["_urns"]
    changes = []
    for delta in diff(remote_state, manifest):
        action, urn_str, data = delta
        urn = URN.from_str(delta[1])
        resource_cls = Resource.classes[urn.resource_key]
        data = resource_cls.fetchable_fields(data)
        if data:
            changes.append((action, urn_str, data))
            print((action, urn_str, data))
    return sorted(changes, key=lambda change: sort_order[change[1]])
