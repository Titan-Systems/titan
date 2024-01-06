import json

from typing import List, Optional, Tuple, Type, Union
from queue import Queue

from . import data_provider

from .client import execute
from .diff import diff, DiffAction
from .identifiers import URN
from .resources.base import (
    Account,
    AccountScoped,
    Database,
    DatabaseScoped,
    OrganizationScoped,
    Resource,
    SchemaScoped,
    Schema,
)
from .resources.validators import coerce_from_str


def print_diffs(diffs):
    for action, target, deltas in diffs:
        print(f"[{action}]", target)
        for delta in deltas:
            print("\t", delta)


def _hash(data):
    return hash(json.dumps(data, sort_keys=True))


def _split_by_scope(
    resources: List[Resource],
) -> Tuple[List[OrganizationScoped], List[AccountScoped], List[DatabaseScoped], List[SchemaScoped]]:
    org_scoped = []
    acct_scoped = []
    db_scoped = []
    schema_scoped = []
    for resource in resources:
        if isinstance(resource, OrganizationScoped):
            org_scoped.append(resource)
        elif isinstance(resource, AccountScoped):
            acct_scoped.append(resource)
        elif isinstance(resource, DatabaseScoped):
            db_scoped.append(resource)
        elif isinstance(resource, SchemaScoped):
            schema_scoped.append(resource)
        else:
            raise Exception(f"Unsupported resource type {type(resource)}")
    return org_scoped, acct_scoped, db_scoped, schema_scoped


def _filter_unfetchable_fields(data):
    filtered = data.copy()
    for key in list(filtered.keys()):
        # field = cls.model_fields[key]
        # fetchable = field.json_schema_extra is None or field.json_schema_extra.get("fetchable", True)
        # FIXME
        fetchable = True
        if not fetchable:
            del filtered[key]
    return filtered


def _plan(remote_state, manifest):
    manifest = manifest.copy()
    urns = manifest.pop("_urns") + list(remote_state.keys())
    sort_order = topological_sort(manifest, urns)
    del manifest["_refs"]
    changes = []
    for action, urn_str, data in diff(remote_state, manifest):
        data = _filter_unfetchable_fields(data)
        if data:
            changes.append((action, urn_str, data))
    return sorted(changes, key=lambda change: sort_order[change[1]])


class Blueprint:
    def __init__(
        self,
        name: str,
        account: Union[None, str, Account] = None,
        database: Union[None, str, Database] = None,
        schema: Union[None, str, Schema] = None,
        resources: List[Resource] = [],
    ) -> None:
        self._finalized = False
        self.name = name
        self.staged: List[Resource] = []
        self.account: Optional[Account] = coerce_from_str(Account)(account) if account else None
        self.database: Optional[Database] = coerce_from_str(Database)(database) if database else None
        self.schema: Optional[Schema] = coerce_from_str(Schema)(schema) if schema else None

        self.add(resources or [])
        self.add([res for res in [self.account, self.database, self.schema] if res is not None])

    def _finalize(self, session_context: dict):
        if self._finalized:
            return
        self._finalized = True

        org_scoped, acct_scoped, db_scoped, schema_scoped = _split_by_scope(self.staged)

        if len(org_scoped) > 1:
            raise Exception("Only one account allowed")
        elif len(org_scoped) == 1:
            # If we have a staged account, use it
            root = org_scoped[0]
        else:
            # Otherwise, use the session context to create a new account
            root = Account(name=session_context["account"], stub=True)
            self.account = root

        # Add all databases and other account scoped resources to the root account
        for resource in acct_scoped:
            root.add(resource)
        if session_context.get("database") is not None:
            root.add(Database(name=session_context["database"], stub=True))

        root_databases = root.databases()

        # Add all schemas and database roles to their respective databases
        for resource in db_scoped:
            if resource.database is None:
                if len(root_databases) == 1:
                    root_databases[0].add(resource)
                else:
                    raise Exception(f"Database [{resource.database}] for resource {resource} not found")

        for resource in schema_scoped:
            if resource.schema is None:
                if len(root_databases) == 1:
                    public_schema = root_databases[0].find(schema="PUBLIC")
                    if public_schema:
                        public_schema.add(resource)
                else:
                    raise Exception(f"No schema for resource {repr(resource)} found")

    def generate_manifest(self, session_context: dict = {}):
        manifest = {}
        refs = []
        urns = []

        self._finalize(session_context)

        # TODO: move this out into it's own function without the session
        for resource in self.staged:
            if resource.implicit:
                continue
            urn = URN.from_resource(account=self.account, resource=resource)

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
                ref_urn = URN.from_resource(account=self.account, resource=ref)
                refs.append((str(urn), str(ref_urn)))
        manifest["_refs"] = refs
        manifest["_urns"] = urns
        return manifest

    def plan(self, session):
        session_ctx = data_provider.fetch_session(session)
        manifest = self.generate_manifest(session_ctx)
        remote_state = data_provider.fetch_remote_state(session, manifest)
        return _plan(remote_state, manifest)

    def apply(self, session, plan=None):
        if plan is None:
            plan = self.plan(session)

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
        session_ctx = data_provider.fetch_session(session)
        manifest = manifest or self.generate_manifest(session_ctx)
        for urn_str in manifest.keys():
            urn = URN.from_str(urn_str)
            resource_cls = Resource.classes[urn.resource_key]
            execute(session, resource_cls.lifecycle_delete(urn.fqn))

    def _add(self, resource):
        if self._finalized:
            raise Exception("Cannot add resources to a finalized blueprint")
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
