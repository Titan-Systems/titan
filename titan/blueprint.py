from typing import List, Optional, Union
from queue import Queue

import snowflake.connector

from . import data_provider, lifecycle
from .client import ALREADY_EXISTS_ERR, INVALID_GRANT_ERR, execute
from .diff import diff, DiffAction
from .enums import ResourceType
from .logical_grant import And, LogicalGrant, Or
from .identifiers import URN, FQN
from .parse import parse_URN
from .privs import (
    CREATE_PRIV_FOR_RESOURCE_TYPE,
    GlobalPriv,
    DatabasePriv,
    RolePriv,
    SchemaPriv,
    priv_for_principal,
    is_ownership_priv,
)
from .resources import Account, Database, Schema
from .resources.resource import Resource, ResourceContainer, ResourcePointer, convert_to_resource
from .scope import AccountScope, DatabaseScope, OrganizationScope, SchemaScope


class MissingPrivilegeException(Exception):
    pass


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
    marked_for_replacement = set()
    for action, urn_str, data in diff(remote_state, manifest):
        urn = parse_URN(urn_str)

        if urn.resource_type == ResourceType.FUTURE_GRANT and action in (DiffAction.ADD, DiffAction.CHANGE):
            for on_type, privs in data.items():
                privs_to_add = privs
                if action == DiffAction.CHANGE:
                    privs_to_add = [priv for priv in privs if priv not in remote_state[urn_str].get(on_type, [])]
                for priv in privs_to_add:
                    changes.append((DiffAction.ADD, urn_str, {on_type: [priv]}))
        elif action == DiffAction.CHANGE:
            if urn_str in marked_for_replacement:
                continue

            # TODO: if the attr is marked as must_replace, then instead we yield a rename, add, remove
            attr = list(data.keys())[0]
            resource_cls = Resource.resolve_resource_cls(urn.resource_type, remote_state[urn_str])
            attr_metadata = resource_cls.spec.get_metadata(attr)
            if attr_metadata.get("triggers_replacement", False):
                marked_for_replacement.add(urn_str)
            else:
                changes.append((action, urn_str, data))
        else:
            changes.append((action, urn_str, data))

    for urn_str in marked_for_replacement:
        changes.append((DiffAction.REMOVE, urn_str, remote_state[urn_str]))
        changes.append((DiffAction.ADD, urn_str, manifest[urn_str]))

    return sorted(changes, key=lambda change: sort_order[change[1]])


def _walk(resource: Resource):
    yield resource
    if isinstance(resource, ResourceContainer):
        for item in resource.items():
            yield from _walk(item)


def _collect_required_privs(session_ctx, plan) -> list:
    """
    For each action in the plan, generate a
    """
    required_priv_list = []

    account_urn = URN.from_session_ctx(session_ctx)

    for action, urn_str, data in plan:
        urn = parse_URN(urn_str)
        resource_cls = Resource.resolve_resource_cls(urn.resource_type, data)
        # privs = []
        privs = And()
        if action == DiffAction.ADD:
            # Special cases

            # GRANT ROLE
            # For example, to create a RoleGrant you need OWNERSHIP on the role.
            if urn.resource_type == ResourceType.ROLE_GRANT:
                role_urn = URN(
                    resource_type=ResourceType.ROLE,
                    fqn=FQN(urn.fqn.name),
                    account_locator=session_ctx["account_locator"],
                )
                privs = privs & (
                    LogicalGrant(role_urn, RolePriv.OWNERSHIP) | LogicalGrant(account_urn, GlobalPriv.MANAGE_GRANTS)
                )

            if isinstance(resource_cls.scope, DatabaseScope):
                privs = privs & (
                    LogicalGrant(urn.database(), DatabasePriv.USAGE)
                    | LogicalGrant(urn.database(), DatabasePriv.OWNERSHIP)
                )
            elif isinstance(resource_cls.scope, SchemaScope):
                privs = (
                    privs
                    & (
                        LogicalGrant(urn.database(), DatabasePriv.USAGE)
                        | LogicalGrant(urn.database(), DatabasePriv.OWNERSHIP)
                    )
                    & (LogicalGrant(urn.schema(), SchemaPriv.USAGE) | LogicalGrant(urn.schema(), SchemaPriv.OWNERSHIP))
                )

            create_priv = CREATE_PRIV_FOR_RESOURCE_TYPE.get(urn.resource_type)
            if create_priv:
                if isinstance(create_priv, GlobalPriv):
                    principal = account_urn
                elif isinstance(create_priv, DatabasePriv):
                    principal = urn.database()
                elif isinstance(create_priv, SchemaPriv):
                    principal = urn.schema()
                else:
                    raise Exception(f"Unsupported privilege type {type(create_priv)}")
                privs = privs & LogicalGrant(principal, create_priv)

        required_priv_list.append(privs)

    return required_priv_list


def _collect_available_privs(session_ctx, session, plan, usable_roles):
    """
    The `priv_map` dictionary structure:
    {
        "SOME_ROLE": {
            "urn::ABC123:database/SOMEDB": {
                privilege1,
                privilege2,
                ...
            },
            ...
        },
        ...
    }
    """
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

    for role in usable_roles:
        priv_map[role] = {}

        if role.startswith("SNOWFLAKE.LOCAL"):
            continue

        # Existing privilege grants
        role_grants = data_provider.fetch_role_grants(session, role)
        if role_grants:
            for principal, grant_list in role_grants.items():
                for grant in grant_list:
                    priv = priv_for_principal(parse_URN(principal), grant["priv"])
                    _add(role, principal, priv)

        # Implied privilege grants in the context of our plan
        for action, urn_str, data in plan:
            urn = parse_URN(urn_str)

            # If we plan to add a new resource and we have the privs to create it, we can assume
            # that we have the OWNERSHIP priv on that resource
            if action == DiffAction.ADD:
                resource_cls = Resource.resolve_resource_cls(urn.resource_type, data)
                create_priv = CREATE_PRIV_FOR_RESOURCE_TYPE.get(urn.resource_type)
                if create_priv is None:
                    continue

                if isinstance(resource_cls.scope, AccountScope):
                    parent_urn = account_urn
                elif isinstance(resource_cls.scope, DatabaseScope):
                    parent_urn = urn.database()
                elif isinstance(resource_cls.scope, SchemaScope):
                    parent_urn = urn.schema()
                else:
                    raise Exception(f"Unsupported resource type {type(resource_cls)}")
                if _contains(role, str(parent_urn), create_priv):
                    ownership_priv = priv_for_principal(urn, "OWNERSHIP")
                    _add(role, urn_str, ownership_priv)
                    if urn.resource_type == ResourceType.DATABASE and urn.fqn.name != "SNOWFLAKE":
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


# TODO
def _raise_if_missing_privs(required: list, available: dict):
    return
    missing = []
    for expr in required:
        pass

    if missing:
        raise MissingPrivilegeException(f"Missing privileges")  #  for {principal}: {required_privs}

    # for principal, privs in required.items():
    #     required_privs = privs.copy()
    #     for priv_map in available.values():
    #         if principal in priv_map:
    #             # If OWNERSHIP in priv_map[principal], we can assume we pass requirements
    #             for priv in priv_map[principal]:
    #                 if is_ownership_priv(priv):
    #                     required_privs = set()
    #                     break
    #             required_privs -= priv_map[principal]
    #     if required_privs:
    #         raise MissingPrivilegeException(f"Missing privileges for {principal}: {required_privs}")


def _fetch_remote_state(session, manifest):
    state = {}
    urns = set(manifest["_urns"].copy())

    # FIXME
    session.cursor().execute("USE ROLE ACCOUNTADMIN")

    for urn_str, _data in manifest.items():
        if urn_str.startswith("_"):
            continue
        urns.remove(urn_str)
        urn = parse_URN(urn_str)
        data = data_provider.fetch_resource(session, urn)
        if urn_str in manifest and data is not None:
            resource_cls = Resource.resolve_resource_cls(urn.resource_type, data)
            if urn.resource_type == ResourceType.FUTURE_GRANT:
                normalized = data
            elif isinstance(data, list):
                normalized = [resource_cls.defaults() | d for d in data]
            else:
                normalized = resource_cls.defaults() | data
            state[urn_str] = normalized

    for urn_str in urns:
        urn = parse_URN(urn_str)
        resource_cls = Resource.resolve_resource_cls(urn.resource_type)
        data = data_provider.fetch_resource(session, urn)
        if data is not None:
            if urn.resource_type == ResourceType.FUTURE_GRANT:
                normalized = data
            elif isinstance(data, list):
                normalized = [resource_cls.defaults() | d for d in data]
            else:
                normalized = resource_cls.defaults() | data
            state[urn_str] = normalized

    return state


class Blueprint:
    def __init__(
        self,
        name: str = None,
        account: Union[None, str, Account] = None,
        database: Union[None, str, Database] = None,
        schema: Union[None, str, Schema] = None,
        resources: List[Resource] = [],
        dry_run: bool = False,
        allow_role_switching: bool = True,
        enforce_requirements: bool = False,
        resource_types: List[ResourceType] = [],
    ) -> None:
        self._finalized = False
        self._staged: List[Resource] = []
        self._root: Account = None
        self._account_locator: str = None
        self._dry_run: bool = dry_run
        self._allow_role_switching: bool = allow_role_switching
        self._enforce_requirements: bool = enforce_requirements
        self._resource_types: List[ResourceType] = resource_types

        self.name = name
        self.account: Optional[Account] = convert_to_resource(Account, account) if account else None
        self.database: Optional[Database] = convert_to_resource(Database, database) if database else None
        self.schema: Optional[Schema] = convert_to_resource(Schema, schema) if schema else None

        if self.account and self.database:
            self.account.add(self.database)

        if self.database and self.schema:
            self.database.add(self.schema)

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
            self._root = ResourcePointer(name=session_context["account"], resource_type=ResourceType.ACCOUNT)
            self._account_locator = session_context["account_locator"]
            self.account = self._root

        # Add all databases and other account scoped resources to the root
        for resource in acct_scoped:
            self._root.add(resource)

        # If we haven't specified a database, use the one from the session context
        if self.database is None and session_context.get("database") is not None:
            existing_databases = [db.name for db in self._root.items(resource_type=ResourceType.DATABASE)]
            if session_context["database"] not in existing_databases:
                self._root.add(ResourcePointer(name=session_context["database"], resource_type=ResourceType.DATABASE))

        databases: list[Database] = self._root.items(resource_type=ResourceType.DATABASE)

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
            elif isinstance(resource.container, ResourcePointer):
                # TODO: clean this up
                found = False
                for db in databases:
                    for schema in db.items(resource_type=ResourceType.SCHEMA):
                        if schema.name == resource.container.name:
                            schema.add(resource)
                            found = True
                            break
                    if found:
                        break
                if not found:
                    raise Exception(f"Schema [{resource.container}] for resource {resource} not found")

    def generate_manifest(self, session_context: dict = {}):
        manifest = {}
        refs = []
        urns = []

        self._finalize(session_context)

        for resource in _walk(self._root):
            if isinstance(resource, Resource) and resource.implicit:
                continue

            urn = URN(
                resource_type=resource.resource_type,
                fqn=resource.fqn,
                account_locator=self._account_locator,
            )

            data = resource.to_dict()

            if isinstance(resource, ResourcePointer):
                data["_pointer"] = True

            manifest_key = str(urn)

            #### Special Cases
            if resource.resource_type == ResourceType.GRANT:
                if manifest_key not in manifest:
                    manifest[manifest_key] = []
                manifest[manifest_key].append(data)
            elif resource.resource_type == ResourceType.FUTURE_GRANT:
                # Role up FUTURE GRANTS on the same role/target to a single entry
                # TODO: support grant option, use a single character prefix on the priv
                if manifest_key not in manifest:
                    manifest[manifest_key] = {}
                on_type = data["on_type"].lower()
                if on_type not in manifest[manifest_key]:
                    manifest[manifest_key][on_type] = []
                if data["priv"] in manifest[manifest_key][on_type]:
                    # raise Exception(f"Duplicate resource {urn} with conflicting data")
                    continue
                manifest[manifest_key][on_type].append(data["priv"])

            #### Normal Case
            else:
                if manifest_key in manifest:
                    if data != manifest[manifest_key]:
                        # raise Exception(f"Duplicate resource {urn} with conflicting data")
                        continue
                manifest[manifest_key] = data

            urns.append(manifest_key)

            print(urn, resource)

            for ref in resource.refs:
                ref_urn = URN.from_resource(account_locator=self._account_locator, resource=ref)
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
        usable_roles = session_ctx["available_roles"] if self._allow_role_switching else [session_ctx["role"]]
        required_privs = _collect_required_privs(session_ctx, plan)
        available_privs = _collect_available_privs(session_ctx, session, plan, usable_roles)

        _raise_if_missing_privs(required_privs, available_privs)

        # print(self._staged)
        # print(plan)

        action_queue = []
        actions_taken = []

        def _queue_action(urn, data, props):
            if action == DiffAction.ADD:
                switch_to_role = None
                if "owner" in data:
                    switch_to_role = data["owner"]
                elif urn.resource_type == ResourceType.FUTURE_GRANT:
                    switch_to_role = "SECURITYADMIN"
                if switch_to_role and switch_to_role in usable_roles:
                    action_queue.append(f"USE ROLE {switch_to_role}")
                else:
                    raise Exception(f"Role {data['owner']} required for {urn} but isn't available")
                action_queue.append(lifecycle.create_resource(urn, data, props))
            elif action == DiffAction.CHANGE:
                action_queue.append(lifecycle.update_resource(urn, data, props))
            elif action == DiffAction.REMOVE:
                action_queue.append(lifecycle.drop_resource(urn, data))

        for action, urn_str, data in plan:
            urn = parse_URN(urn_str)
            props = Resource.props_for_resource_type(urn.resource_type, data)
            _queue_action(urn, data, props)

        while action_queue:
            sql = action_queue.pop(0)
            actions_taken.append(sql)
            try:
                if not self._dry_run:
                    execute(session, sql)
            except snowflake.connector.errors.ProgrammingError as err:
                if err.errno == ALREADY_EXISTS_ERR:
                    print(f"Resource already exists: {urn_str}, skipping...")
                elif err.errno == INVALID_GRANT_ERR:
                    print(f"Invalid grant: {urn_str}, skipping...")
                else:
                    raise err
        return actions_taken

    def destroy(self, session, manifest=None):
        session_ctx = data_provider.fetch_session(session)
        manifest = manifest or self.generate_manifest(session_ctx)
        for urn_str, data in manifest.items():
            if urn_str.startswith("_"):
                continue

            if isinstance(data, dict) and data.get("_pointer"):
                continue
            urn = parse_URN(urn_str)
            if urn.resource_type == ResourceType.GRANT:
                for grant in data:
                    execute(session, lifecycle.drop_resource(urn, grant))
            else:
                try:
                    execute(session, lifecycle.drop_resource(urn, data))
                except snowflake.connector.errors.ProgrammingError as err:
                    print("failed")
                    continue

    def _add(self, resource: Resource):
        if self._finalized:
            raise Exception("Cannot add resources to a finalized blueprint")
        if not isinstance(resource, Resource):
            raise Exception(f"Expected a Resource, got {type(resource)} -> {resource}")
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
