import json

from collections import defaultdict
from typing import List, Optional, Union
from queue import Queue

import snowflake.connector

from . import data_provider, lifecycle
from .client import ALREADY_EXISTS_ERR, execute
from .diff import diff, DiffAction
from .identifiers import URN, FQN
from .parse import parse_URN
from .privs import (
    GlobalPriv,
    DatabasePriv,
    SchemaPriv,
    priv_for_principal,
    is_ownership_priv,
    create_priv_for_resource_type,
)
from .resources import Account, Database, Schema
from .resources.resource import Resource, ResourceContainer, ResourceType
from .resources.validators import coerce_from_str
from .scope import AccountScope, DatabaseScope, OrganizationScope, SchemaScope


def print_diffs(diffs):
    for action, target, deltas in diffs:
        print(f"[{action}]", target)
        for delta in deltas:
            print("\t", delta)


def _split_by_scope(
    resources: list[Resource],
) -> tuple[list[Resource], list[Resource], list[Resource], list[Resource]]:
    org_scoped = []
    acct_scoped = []
    db_scoped = []
    schema_scoped = []

    def route(resource: Resource):
        """The sorting hat"""
        if isinstance(resource.scope, OrganizationScope):
            org_scoped.append(resource)
        elif isinstance(resource.scope, AccountScope):
            acct_scoped.append(resource)
        elif isinstance(resource.scope, DatabaseScope):
            db_scoped.append(resource)
        elif isinstance(resource.scope, SchemaScope):
            schema_scoped.append(resource)
        else:
            raise Exception(f"Unsupported resource type {type(resource)}")

    for resource in resources:
        route(resource)
        if isinstance(resource, ResourceContainer):
            for item in resource.items():
                route(item)
    return org_scoped, acct_scoped, db_scoped, schema_scoped


def _plan(remote_state, manifest):
    manifest = manifest.copy()

    # Generate a list of all URNs
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
        changes.append((action, urn_str, data))
    return sorted(changes, key=lambda change: sort_order[change[1]])


def _walk(resource: Resource):
    yield resource
    if isinstance(resource, ResourceContainer):
        for item in resource.items():
            yield from _walk(item)


def _collect_required_privs(session_ctx, plan):
    priv_map = defaultdict(set)

    def _add(urn, priv):
        priv_map[str(urn)].add(priv)

    account_urn = URN.from_session_ctx(session_ctx)

    for action, urn_str, data in plan:
        urn = parse_URN(urn_str)
        privs = []
        if action == DiffAction.ADD:
            privs = lifecycle.privs_for_create(urn, data)

        for priv in privs:
            if isinstance(priv, GlobalPriv):
                _add(account_urn, priv)
            elif isinstance(priv, DatabasePriv):
                _add(urn.database(), priv)
            elif isinstance(priv, SchemaPriv):
                _add(urn.schema(), priv)

    return dict(priv_map)


def _collect_available_privs(session_ctx, session, plan):
    priv_map = {}

    def _add(role, principal, priv):
        if role not in priv_map:
            priv_map[role] = {}
        if principal not in priv_map[role]:
            priv_map[role][principal] = set()
        priv_map[role][principal].add(priv)

    def _contains(role, principal, priv):
        if role not in priv_map:
            return False
        if principal not in priv_map[role]:
            return False
        return priv in priv_map[role][principal]

    account_urn = URN.from_session_ctx(session_ctx)

    for role in session_ctx["available_roles"]:
        priv_map[role] = {}

        if role.startswith("SNOWFLAKE.LOCAL"):
            continue

        # Existing privilege grants
        role_grants = data_provider.fetch_role_grants(session, role)
        for principal, grant_list in role_grants.items():
            for grant in grant_list:
                priv = priv_for_principal(parse_URN(principal), grant["priv"])
                _add(role, principal, priv)

        # Implied privilege grants in the context of our plan
        for action, urn_str, _ in plan:
            urn = parse_URN(urn_str)
            # If we plan to add a new resource and we have the privs to create it, we can assume
            # that we have the OWNERSHIP priv on that resource
            if action == DiffAction.ADD:
                create_priv = create_priv_for_resource_type(urn.resource_type)
                if create_priv is None:
                    continue
                ownership_priv = priv_for_principal(urn, "OWNERSHIP")
                if urn.resource_type == ResourceType.DATABASE:
                    parent_urn = account_urn
                elif urn.resource_type == ResourceType.SCHEMA:
                    parent_urn = urn.database()
                else:
                    parent_urn = urn.schema()
                if _contains(role, str(parent_urn), create_priv):
                    _add(role, urn_str, ownership_priv)
                    if urn.resource_type == ResourceType.DATABASE:
                        public_schema = URN(
                            account_locator=account_urn.account_locator,
                            resource_type=ResourceType.SCHEMA,
                            fqn=FQN(name="PUBLIC", database=urn.fqn.name),
                        )
                        information_schema = URN(
                            account_locator=account_urn.account_locator,
                            resource_type=ResourceType.SCHEMA,
                            fqn=FQN(name="PUBLIC", database=urn.fqn.name),
                        )
                        _add(role, str(public_schema), priv_for_principal(public_schema, "OWNERSHIP"))
                        _add(role, str(information_schema), priv_for_principal(information_schema, "OWNERSHIP"))

    return priv_map


def _raise_if_missing_privs(required: dict, available: dict):
    for principal, privs in required.items():
        required_privs = privs.copy()
        for priv_map in available.values():
            if principal in priv_map:
                # If OWNERSHIP in priv_map[principal], we can assume we pass requirements
                for priv in priv_map[principal]:
                    if is_ownership_priv(priv):
                        required_privs = set()
                        break
                required_privs -= priv_map[principal]
        if required_privs:
            raise Exception(f"Missing privileges for {principal}: {required_privs}")


def _fetch_remote_state(session, manifest):
    state = {}
    for urn_str in manifest["_urns"]:
        urn = parse_URN(urn_str)
        resource_cls = Resource.resolve_resource_cls(urn.resource_type)
        data = data_provider.fetch_resource(session, urn)
        if urn_str in manifest and data is not None:
            if isinstance(data, list):
                normalized = [resource_cls.defaults() | d for d in data]
            else:
                normalized = resource_cls.defaults() | data
            state[urn_str] = normalized

    return state


class Blueprint:
    def __init__(
        self,
        name: str,
        account: Union[None, str, Account] = None,
        database: Union[None, str, Database] = None,
        schema: Union[None, str, Schema] = None,
        resources: List[Resource] = [],
        enforce_requirements: bool = False,
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

        databases: list[Database] = self._root.databases()

        # Add all schemas and database roles to their respective databases
        for resource in db_scoped:
            if resource.container is None:
                if len(databases) == 1:
                    databases[0].add(resource)
                else:
                    raise Exception(f"Database [{resource.container}] for resource {resource} not found")

        for resource in schema_scoped:
            if resource.container is None:
                if len(databases) == 1:
                    public_schema: Schema = databases[0].find(resource_type=ResourceType.SCHEMA, name="PUBLIC")
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
            data = resource.to_dict()

            if resource.stub:
                data["_stub"] = True

            manifest_key = str(urn)

            if resource.resource_type == ResourceType.GRANT:
                if manifest_key not in manifest:
                    manifest[manifest_key] = []
                manifest[manifest_key].append(data)
            else:
                if manifest_key in manifest:
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
        remote_state = _fetch_remote_state(session, manifest)
        return _plan(remote_state, manifest)

    def apply(self, session, plan=None):
        if plan is None:
            plan = self.plan(session)

        # TODO: cursor setup, including query tag
        # TODO: clean up urn vs urn_str madness

        """
            At this point, we have a list of actions as a part of the plan. Each action is one of:
                1. [ADD] action (CREATE command)
                2. [CHANGE] action (one or many ALTER or SET PARAMETER commands)
                3. [REMOVE] action (DROP command, REVOKE command, or a rename operation)

            Each action requires:
                • a set of privileges necessary to run commands
                • the appropriate role to execute commands

            Once we've determined those things, we can compare the list of required roles and privileges
            against what we have access to in the session and the role tree.
        """

        session_ctx = data_provider.fetch_session(session)
        required_privs = _collect_required_privs(session_ctx, plan)
        available_privs = _collect_available_privs(session_ctx, session, plan)

        _raise_if_missing_privs(required_privs, available_privs)

        action_queue = []
        actions_taken = []

        def _queue_action(urn, data, props):
            if action == DiffAction.ADD:
                action_queue.append(lifecycle.create_resource(urn, data, props))
            elif action == DiffAction.CHANGE:
                action_queue.append(lifecycle.update_resource(urn, data, props))
            elif action == DiffAction.REMOVE:
                action_queue.append(lifecycle.drop_resource(urn, data))

        for action, urn_str, data in plan:
            urn = parse_URN(urn_str)

            props = Resource.props_for_resource_type(urn.resource_type)

            _queue_action(urn, data, props)

            while action_queue:
                sql = action_queue.pop(0)
                actions_taken.append(sql)
                try:
                    execute(session, sql)
                except snowflake.connector.errors.ProgrammingError as err:
                    if err.errno == ALREADY_EXISTS_ERR:
                        print(f"Resource already exists: {urn_str}, skipping...")
                    raise err
        return actions_taken

    def destroy(self, session, manifest=None):
        session_ctx = data_provider.fetch_session(session)
        manifest = manifest or self.generate_manifest(session_ctx)
        for urn_str, data in manifest.items():
            urn = parse_URN(urn_str)
            if urn.resource_type == ResourceType.GRANT:
                for grant in data:
                    execute(session, lifecycle.drop_resource(urn, grant))
            else:
                execute(session, lifecycle.drop_resource(urn, data))

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
