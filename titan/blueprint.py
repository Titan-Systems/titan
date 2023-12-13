import json

from typing import List
from queue import Queue

from . import data_provider

from .client import execute
from .diff import diff, DiffAction
from .identifiers import URN
from .resources.base import (
    Account,
    AccountScoped,
    DatabaseScoped,
    OrganizationScoped,
    Resource,
    SchemaScoped,
)


def print_diffs(diffs):
    for action, target, deltas in diffs:
        print(f"[{action}]", target)
        for delta in deltas:
            print("\t", delta)


def _hash(data):
    return hash(json.dumps(data, sort_keys=True))


class Blueprint:
    def __init__(self, name, organization=None, account=None, database=None, schema=None, resources=[]) -> None:
        self.staged: List[Resource] = []
        self.add(resources or [])
        # NOTE: might want a resolution function here
        self.organization = organization or ""
        self.account = account or ""
        self.database = database or ""
        self.schema = schema or ""

    def generate_manifest(self):
        manifest = {}
        refs = []
        urns = []
        for resource in self.staged:
            if resource.implicit:
                continue
            self._finalize_scope(resource)
            urn = URN.from_resource(organization=self.organization, account=self.account, resource=resource)

            if resource.implicit:
                continue
            data = resource.model_dump(exclude_none=True)

            manifest_key = str(urn)

            if resource.serialize_as_list:
                if manifest_key not in manifest:
                    manifest[manifest_key] = []
                manifest[manifest_key].append(data)
            else:
                if manifest_key in manifest:  # and _hash(manifest[manifest_key]) != _hash(data):
                    # raise RuntimeError(f"Duplicate resource found {manifest_key}")
                    continue
                manifest[manifest_key] = data
            urns.append(manifest_key)

            for ref in resource.refs:
                self._finalize_scope(ref)
                ref_urn = URN.from_resource(organization=self.organization, account=self.account, resource=ref)
                refs.append((str(urn), str(ref_urn)))
        manifest["_refs"] = refs
        manifest["_urns"] = urns
        return manifest

    def _finalize_scope(self, resource: Resource):
        # TODO: connect stubs
        is_scoped = isinstance(resource, (OrganizationScoped, AccountScoped, DatabaseScoped, SchemaScoped))
        if is_scoped and not resource.has_scope():
            if isinstance(resource, OrganizationScoped):
                resource.organization = self.organization
            elif isinstance(resource, AccountScoped):
                resource.account = self.account
            elif isinstance(resource, DatabaseScoped):
                if self.database is None:
                    raise Exception(f"Orphaned resource found {resource}")
                resource.database = self.database
            elif isinstance(resource, SchemaScoped):
                if self.schema is None:
                    raise Exception(f"Orphaned resource found {resource}")
                resource.schema = self.schema

    def plan(self, session):
        manifest = self.generate_manifest()
        remote_state = data_provider.fetch_remote_state(session, manifest)
        return _plan(remote_state, manifest)

    def apply(self, session, plan=None):
        if plan is None:
            plan = self.plan(session)

        # with session.cursor() as cur:
        # TODO: cursor setup, including query tag
        for action, urn_str, data in plan:
            urn = URN.from_str(urn_str)
            resource_cls = Resource.classes[urn.resource_key]
            try:
                if action == DiffAction.ADD:
                    sql = resource_cls.lifecycle_create(urn.fqn, data)
                elif action == DiffAction.CHANGE:
                    sql = resource_cls.lifecycle_update(urn.fqn, data)
                elif action == DiffAction.REMOVE:
                    sql = resource_cls.lifecycle_delete(urn.fqn, data)
                else:
                    raise Exception(f"Unexpected action {action} in plan")
                execute(session, sql)
            except AttributeError as err:
                raise AttributeError(f"Resource {resource_cls.__name__} missing lifecycle action") from err

    def destroy(self, session, manifest=None):
        manifest = manifest or self.generate_manifest()
        for urn_str in manifest.keys():
            urn = URN.from_str(urn_str)
            resource_cls = Resource.classes[urn.resource_key]
            execute(session, resource_cls.lifecycle_delete(urn.fqn))

    def _add(self, resource):
        if isinstance(resource, Account):
            if self.account == "":
                self.account = resource.name
            else:
                # TODO: out of scope exception
                raise Exception("Account already set")
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
    for action, urn_str, data in diff(remote_state, manifest):
        urn = URN.from_str(urn_str)
        resource_cls = Resource.classes[urn.resource_key]
        data = resource_cls.fetchable_fields(data)
        if data:
            changes.append((action, urn_str, data))
    return sorted(changes, key=lambda change: sort_order[change[1]])
