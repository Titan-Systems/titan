import json

from typing import List, Optional, Tuple, Type, Union
from queue import Queue

from . import data_provider, lifecycle

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


def _is_container(resource):
    return hasattr(resource, "children") and callable(resource.children)


def _split_by_scope(
    resources: List[Resource],
) -> Tuple[List[OrganizationScoped], List[AccountScoped], List[DatabaseScoped], List[SchemaScoped]]:
    org_scoped = []
    acct_scoped = []
    db_scoped = []
    schema_scoped = []

    def route(resource):
        """The sorting hat"""
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

    for resource in resources:
        route(resource)
        if _is_container(resource):
            for child in resource.children():
                route(child)
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

    # Generate a list of all URNs we're concerned with
    resource_set = set(manifest["_urns"] + list(remote_state.keys()))

    for ref in manifest["_refs"]:
        resource_set.add(ref[0])
        resource_set.add(ref[1])

    # Calculate a topological sort order for the URNs
    sort_order = topological_sort(resource_set, manifest["_refs"])

    # Once sorting is done, remove the _refs and _urns keys from the manifest
    del manifest["_refs"]
    del manifest["_urns"]

    changes = []
    for action, urn_str, data in diff(remote_state, manifest):
        data = _filter_unfetchable_fields(data)
        if data:
            changes.append((action, urn_str, data))
    return sorted(changes, key=lambda change: sort_order[change[1]])


def _walk(resource: Resource):
    yield resource
    if _is_container(resource):
        for child in resource.children():
            yield from _walk(child)


def _collect_privs(plan):
    privs = {}
    for action, urn_str, data in plan:
        urn = URN.from_str(urn_str)
        if action == DiffAction.ADD:
            privs |= lifecycle.create_resource_privs(urn, data)
    return privs


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
        self._staged: List[Resource] = []
        self._root: Account = None
        self.name = name
        self.account: Optional[Account] = coerce_from_str(Account)(account) if account else None
        self.database: Optional[Database] = coerce_from_str(Database)(database) if database else None
        self.schema: Optional[Schema] = coerce_from_str(Schema)(schema) if schema else None

        self.add(resources or [])
        self.add([res for res in [self.account, self.database, self.schema] if res is not None])

    def _finalize(self, session_context: dict):
        """
        Convert the staged resources into a tree of resources
        """
        if self._finalized:
            return
        self._finalized = True

        org_scoped, acct_scoped, db_scoped, schema_scoped = _split_by_scope(self._staged)

        if len(org_scoped) > 1:
            raise Exception("Only one account allowed")
        elif len(org_scoped) == 1:
            # If we have a staged account, use it
            self._root = org_scoped[0]
        else:
            # Otherwise, create a stub account from the session context
            self._root = Account(
                name=session_context["account"],
                locator=session_context["account_locator"],
                stub=True,
            )
            self.account = self._root

        # Add all databases and other account scoped resources to the root
        for resource in acct_scoped:
            self._root.add(resource)
        if session_context.get("database") is not None:
            self._root.add(Database(name=session_context["database"], stub=True))

        databases = self._root.databases()

        # Add all schemas and database roles to their respective databases
        for resource in db_scoped:
            if resource.database is None:
                if len(databases) == 1:
                    databases[0].add(resource)
                else:
                    raise Exception(f"Database [{resource.database}] for resource {resource} not found")

        for resource in schema_scoped:
            if resource.schema is None:
                if len(databases) == 1:
                    public_schema = databases[0].find(schema="PUBLIC")
                    if public_schema:
                        public_schema.add(resource)
                else:
                    raise Exception(f"No schema for resource {repr(resource)} found")

    def generate_manifest(self, session_context: dict = {}):
        manifest = {}
        refs = []
        urns = []

        self._finalize(session_context)

        for resource in _walk(self._root):
            if resource.implicit:
                continue

            urn = URN.from_resource(account_locator=self.account.locator, resource=resource)
            data = resource.model_dump(exclude_none=True)

            if resource.stub:
                data["_stub"] = True

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
                ref_urn = URN.from_resource(account_locator=self.account.locator, resource=ref)
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

        """
            At this point, we have a list of actions as a part of the plan. Each action is one of:
                1. [ADD] action (CREATE command)
                2. [CHANGE] action (one or many ALTER or SET PARAMETER commands)
                3. [REMOVE] action (DROP command, REVOKE command, or a rename operation)

            Each action requires:
                • a set of privileges necessary to run commands
                • the appropriate role to execute commands

            Once we've determined those things, we can compare the list of required roles and privileges
            against what we have access to in the session and the role tree.
        """

        # TODO: perform a privilege analysis (grant map)
        required_privs = _collect_privs(plan)
        session_ctx = data_provider.fetch_session(session)
        available_privs = {}
        for role in session_ctx["available_roles"]:
            role_privs = data_provider.fetch_role_privs(session, role)
            available_privs[role] = role_privs
            # available_privs |= role_privs
        print(required_privs)
        print(available_privs)

        for action, urn_str, data in plan:
            urn = URN.from_str(urn_str)

            if action == DiffAction.ADD:
                sql = lifecycle.create_resource(urn, data)
            elif action == DiffAction.CHANGE:
                sql = lifecycle.update_resource(urn, data)
            elif action == DiffAction.REMOVE:
                sql = lifecycle.drop_resource(urn, data)
            execute(session, sql)

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
        self._staged.append(resource)

    def add(self, *resources):
        if isinstance(resources[0], list):
            resources = resources[0]
        for resource in resources:
            self._add(resource)


def topological_sort(resource_set: set, references: list):
    # Kahn's algorithm

    # Compute in-degree (# of inbound edges) for each node
    in_degrees = {}
    outgoing_edges = {}

    for node in resource_set:
        in_degrees[node] = 0
        outgoing_edges[node] = set()

    for node, ref in references:
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
