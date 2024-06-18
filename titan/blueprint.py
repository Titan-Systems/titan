import json
import logging

from dataclasses import dataclass
from typing import List, Optional, Union
from queue import Queue

import snowflake.connector

from . import data_provider, lifecycle
from .builtins import SYSTEM_DATABASES, SYSTEM_ROLES
from .client import ALREADY_EXISTS_ERR, INVALID_GRANT_ERR, execute, reset_cache
from .diff import diff, Action
from .enums import ResourceType, ParseableEnum
from .logical_grant import And, LogicalGrant, Or
from .identifiers import URN, FQN, resource_label_for_type
from .parse import parse_URN, parse_identifier
from .privs import (
    CREATE_PRIV_FOR_RESOURCE_TYPE,
    GlobalPriv,
    DatabasePriv,
    RolePriv,
    SchemaPriv,
    priv_for_principal,
    is_ownership_priv,
)
from .resources import Account, Database, Schema, Grant
from .resources.resource import Resource, ResourceContainer, ResourcePointer, convert_to_resource
from .scope import AccountScope, DatabaseScope, OrganizationScope, SchemaScope

logger = logging.getLogger("titan")

Manifest = dict[URN, dict]
State = dict[URN, dict]

class MissingPrivilegeException(Exception):
    pass


class MissingResourceException(Exception):
    pass

class MarkedForReplacementException(Exception):
    pass


class RunMode(ParseableEnum):
    CREATE_OR_UPDATE = "CREATE-OR-UPDATE"
    FULLY_MANAGED = "FULLY-MANAGED"


@dataclass
class ResourceChange:
    action: Action
    urn: URN
    before: dict
    after: dict
    delta: dict

    def to_dict(self):
        return {
            "action": self.action.value,
            "urn": str(self.urn),
            "before": self.before,
            "after": self.after,
            "delta": self.delta,
        }


Plan = list[ResourceChange]

def dump_plan(plan: Plan, format: str = "json"):
    if format == "json":
        return json.dumps([change.to_dict() for change in plan], indent=2)
    elif format == "text":
        """
        account:ABC123

        » role.transformer will be created

        + role "urn::ABC123:role/transformer" {
            + name  = "transformer"
            + owner = "SYSADMIN"
            }

        + warehouse "urn::ABC123:warehouse/transforming" {
            + name           = "transforming"
            + owner          = "SYSADMIN"
            + warehouse_type = "STANDARD"
            + warehouse_size = "LARGE"
            + auto_suspend   = 60
            }

        + grant "urn::ABC123:grant/..." {
            + priv = "USAGE"
            + on   = warehouse "transforming"
            + to   = role "transformer
            }

        + grant "urn::ABC123:grant/..." {
            + priv = "OPERATE"
            + on   = warehouse "transforming"
            + to   = role "transformer
            }
        """
        output = ""

        def _render_value(value):
            if isinstance(value, str):
                return f'"{value}"'
            return str(value)
        
        # green_start = "\033[36m"
        # color_end = "\033[0m"

        add_count = len([change for change in plan if change.action == Action.ADD])
        change_count = len([change for change in plan if change.action == Action.CHANGE])
        remove_count = len([change for change in plan if change.action == Action.REMOVE])

        output += "\n» titan[core]\n"
        output += f"» Plan: {add_count} to add, {change_count} to change, {remove_count} to destroy.\n\n"

        for change in plan:
            action_marker = ""
            if change.action == Action.ADD:
                action_marker = "+"
            elif change.action == Action.CHANGE:
                action_marker = "~"
            elif change.action == Action.REMOVE:
                action_marker = "-"
            output += f"{action_marker} {change.urn}" "{\n"
            key_lengths = [len(key) for key in change.delta.keys()]
            max_key_length = max(key_lengths) if len(key_lengths) > 0 else 0
            for key, value in change.delta.items():
                new_value = _render_value(value)
                before_value = ""
                if key in change.before:
                    before_value = _render_value(change.before[key]) + " -> "
                output += f"  {action_marker} {key:<{max_key_length}} = {before_value}{new_value}\n"
            output += "}" + "\n\n"
        return output
    else:
        raise Exception(f"Unsupported format {format}")

def print_plan(plan: Plan):
    print(dump_plan(plan, format="text"))


def plan_sql(plan: Plan):
    """
    Generate SQL commands based on the plan provided.

    Args:
    plan (Plan): The plan containing changes to be applied to the database.

    Returns:
    List[str]: A list of SQL commands to be executed.
    """
    sql_commands = []
    for change in plan:
        props = Resource.props_for_resource_type(change.urn.resource_type, change.after)
        if change.action == Action.ADD:
            sql_commands.append(lifecycle.create_resource(change.urn, change.after, props))
        elif change.action == Action.CHANGE:
            sql_commands.append(lifecycle.update_resource(change.urn, change.delta, props))
        elif change.action == Action.REMOVE:
            sql_commands.append(lifecycle.drop_resource(change.urn, change.before))
    return sql_commands


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


def _walk(resource: Resource):
    yield resource
    if isinstance(resource, ResourceContainer):
        for item in resource.items():
            yield from _walk(item)


def _collect_required_privs(session_ctx: dict, plan: Plan) -> list:
    """
    For each action in the plan, generate a
    """
    required_priv_list = []

    account_urn = URN.from_session_ctx(session_ctx)

    for change in plan:
        resource_cls = Resource.resolve_resource_cls(change.urn.resource_type, change.after)
        # privs = []
        privs = And()
        if change.action == Action.ADD:
            # Special cases

            # GRANT ROLE
            # For example, to create a RoleGrant you need OWNERSHIP on the role.
            if change.urn.resource_type == ResourceType.ROLE_GRANT:
                role_urn = URN(
                    resource_type=ResourceType.ROLE,
                    fqn=FQN(change.urn.fqn.name),
                    account_locator=session_ctx["account_locator"],
                )
                privs = privs & (
                    LogicalGrant(role_urn, RolePriv.OWNERSHIP) | LogicalGrant(account_urn, GlobalPriv.MANAGE_GRANTS)
                )

            if isinstance(resource_cls.scope, DatabaseScope):
                privs = privs & (
                    LogicalGrant(change.urn.database(), DatabasePriv.USAGE)
                    | LogicalGrant(change.urn.database(), DatabasePriv.OWNERSHIP)
                )
            elif isinstance(resource_cls.scope, SchemaScope):
                privs = (
                    privs
                    & (
                        LogicalGrant(change.urn.database(), DatabasePriv.USAGE)
                        | LogicalGrant(change.urn.database(), DatabasePriv.OWNERSHIP)
                    )
                    & (
                        LogicalGrant(change.urn.schema(), SchemaPriv.USAGE)
                        | LogicalGrant(change.urn.schema(), SchemaPriv.OWNERSHIP)
                    )
                )

            create_priv = CREATE_PRIV_FOR_RESOURCE_TYPE.get(change.urn.resource_type)
            if create_priv:
                if isinstance(create_priv, GlobalPriv):
                    principal = account_urn
                elif isinstance(create_priv, DatabasePriv):
                    principal = change.urn.database()
                elif isinstance(create_priv, SchemaPriv):
                    principal = change.urn.schema()
                else:
                    raise Exception(f"Unsupported privilege type {type(create_priv)}")
                privs = privs & LogicalGrant(principal, create_priv)

        required_priv_list.append(privs)

    return required_priv_list


def _collect_available_privs(session_ctx: dict, session, plan: Plan, usable_roles: list[str]) -> dict:
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

        if role.startswith("SNOWFLAKE.LOCAL") or role.endswith(".ALL_ENDPOINTS_USAGE"):
            continue

        # Existing privilege grants
        role_grants = data_provider.fetch_role_grants(session, role)
        if role_grants:
            for principal, grant_list in role_grants.items():
                for grant in grant_list:
                    try:
                        priv = priv_for_principal(parse_URN(principal), grant["priv"])
                        _add(role, principal, priv)
                    except ValueError:
                        # Priv not recognized by Titan
                        pass

        # Implied privilege grants in the context of our plan
        for change in plan:

            # If we plan to add a new resource and we have the privs to create it, we can assume
            # that we have the OWNERSHIP priv on that resource
            if change.action == Action.ADD:
                resource_cls = Resource.resolve_resource_cls(change.urn.resource_type, change.after)
                create_priv = CREATE_PRIV_FOR_RESOURCE_TYPE.get(change.urn.resource_type)
                if create_priv is None:
                    continue

                if isinstance(resource_cls.scope, AccountScope):
                    parent_urn = account_urn
                elif isinstance(resource_cls.scope, DatabaseScope):
                    parent_urn = change.urn.database()
                elif isinstance(resource_cls.scope, SchemaScope):
                    parent_urn = change.urn.schema()
                else:
                    raise Exception(f"Unsupported resource type {type(resource_cls)}")
                if _contains(role, str(parent_urn), create_priv):
                    ownership_priv = priv_for_principal(change.urn, "OWNERSHIP")
                    _add(role, str(parent_urn), ownership_priv)
                    if change.urn.resource_type == ResourceType.DATABASE and change.urn.fqn.name not in SYSTEM_DATABASES:
                        public_schema = URN(
                            account_locator=account_urn.account_locator,
                            resource_type=ResourceType.SCHEMA,
                            fqn=FQN(name="PUBLIC", database=change.urn.fqn.name),
                        )
                        information_schema = URN(
                            account_locator=account_urn.account_locator,
                            resource_type=ResourceType.SCHEMA,
                            fqn=FQN(name="INFORMATION_SCHEMA", database=change.urn.fqn.name),
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


class Blueprint:
    def __init__(
        self,
        name: str = None,
        account: Union[None, str, Account] = None,
        database: Union[None, str, Database] = None,
        schema: Union[None, str, Schema] = None,
        resources: List[Resource] = None,
        run_mode: RunMode = RunMode.CREATE_OR_UPDATE,
        dry_run: bool = False,
        allow_role_switching: bool = True,
        ignore_ownership: bool = True,
        valid_resource_types: List[ResourceType] = None,
    ) -> None:
        # TODO: input validation

        if not allow_role_switching:
            raise Exception("Role switching must be allowed in this version of Titan")
        
        self._finalized = False
        self._staged: List[Resource] = []
        self._root: Account = None
        self._account_locator: str = None
        self._run_mode: RunMode = RunMode(run_mode)
        self._dry_run: bool = dry_run
        self._allow_role_switching: bool = allow_role_switching
        self._ignore_ownership: bool = ignore_ownership
        self._valid_resource_types: List[ResourceType] = [ResourceType(v) for v in valid_resource_types or []]

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

    def _raise_for_nonconforming_plan(self, plan: Plan):
        exceptions = []

        # Run Mode exceptions
        if self._run_mode == RunMode.CREATE_OR_UPDATE:
            for change in plan:
                if change.action == Action.REMOVE:
                    exceptions.append(
                        f"Create-or-update mode does not allow resources to be removed (ref: {change.urn})"
                    )
                if change.action == Action.CHANGE:
                    if "owner" in change.delta:
                        change_debug = f"{change.before['owner']} => {change.delta['owner']}"
                        exceptions.append(
                            f"Create-or-update mode does not allow ownership changes (resource: {change.urn}, owner: {change_debug})"
                        )
                    elif "name" in change.delta:
                        exceptions.append(
                            f"Create-or-update mode does not allow renaming resources (ref: {change.urn})"
                        )
        # else:
        #     raise Exception(f"Unsupported run mode {self._run_mode}")

        # Valid Resource Types exceptions
        if self._valid_resource_types:
            for change in plan:
                if change.urn.resource_type not in self._valid_resource_types:
                    exceptions.append(f"Resource type {change.urn.resource_type} not allowed in blueprint")

        if exceptions:
            if len(exceptions) > 5:
                exception_block = "\n".join(exceptions[0:5]) + f"\n... and {len(exceptions) - 5} more"
            else:
                exception_block = "\n".join(exceptions)
            raise Exception("Non-conforming actions found in plan:\n" + exception_block)

    def _plan(self, remote_state: State, manifest: Manifest) -> Plan:
        manifest = manifest.copy()
        refs = manifest.pop("_refs")
        urns = manifest.pop("_urns")

        changes: Plan = []
        marked_for_replacement = set()
        for action, urn, delta in diff(remote_state, manifest):
            before = remote_state.get(urn, {})
            after = manifest.get(urn, {})

            if action == Action.CHANGE:
                if urn in marked_for_replacement:
                    continue

                resource_cls = Resource.resolve_resource_cls(urn.resource_type, before)

                # TODO: if the attr is marked as must_replace, then instead we yield a rename, add, remove
                attr = list(delta.keys())[0]
                attr_metadata = resource_cls.spec.get_metadata(attr)
                if attr_metadata.get("triggers_replacement", False):
                    raise MarkedForReplacementException(f"Resource {urn} is marked for replacement", resource_cls, attr)
                    marked_for_replacement.add(urn)
                elif attr_metadata.get("forces_add", False):
                    changes.append(ResourceChange(action=Action.ADD, urn=urn, before={}, after=after, delta=delta))
                    continue
                elif attr_metadata.get("fetchable", True) is False:
                    # drift on fields that aren't fetchable should be ignored
                    # TODO: throw a warning, or have a blueprint runmode that fails on this
                    continue
                elif attr == "owner" and self._ignore_ownership:
                    continue
                else:
                    changes.append(ResourceChange(action, urn, before, after, delta))
            elif action == Action.ADD:
                changes.append(ResourceChange(action=action, urn=urn, before={}, after=after, delta=delta))
            elif action == Action.REMOVE:
                changes.append(ResourceChange(action=action, urn=urn, before=before, after={}, delta={}))

        for urn in marked_for_replacement:
            raise MarkedForReplacementException(f"Resource {urn} is marked for replacement")
            # changes.append(ResourceChange(action=Action.REMOVE, urn=urn, before=before, after={}, delta={}))
            # changes.append(ResourceChange(action=Action.ADD, urn=urn, before={}, after=after, delta=after))

        # # Handle account role ownership by creating an ownership grant
        # for change in changes:
        #     if change.action == Action.ADD and change.after.get("owner") in SYSTEM_ROLES:
        #         changes.append(ResourceChange(action=Action.ADD, urn=change.urn, before={}, after=after, delta=after))
        
        # Generate a list of all URNs
        resource_set = set(urns + list(remote_state.keys()))
        for ref in refs:
            resource_set.add(ref[0])
            resource_set.add(ref[1])
        # Calculate a topological sort order for the URNs
        sort_order = topological_sort(resource_set, set(refs))
        return sorted(changes, key=lambda change: sort_order[change.urn])

    def fetch_remote_state(self, session, manifest: Manifest) -> State:
        state: State = {}

        manifest_urns : set[URN] = set(manifest["_urns"].copy())

        def _normalize(urn: URN, data: dict) -> dict:
            resource_cls = Resource.resolve_resource_cls(urn.resource_type, data)
            if urn.resource_type == ResourceType.FUTURE_GRANT:
                normalized = data
            elif isinstance(data, list):
                raise Exception(f"Fetching list of {urn.resource_type} is not supported yet")
                normalized = [resource_cls.defaults() | d for d in data]
            else:
                normalized = resource_cls.defaults() | data
            return normalized

        if self._run_mode == RunMode.FULLY_MANAGED:
            """
            In fully managed mode, the remote state is not just the resources that were added to the blueprint, 
            but all resources that exist in Snowflake. This is limited by a few things:
            - valid_resource_types limits the scope of what resources types are allowed in a blueprint
            - if database or schema is set, the blueprint only looks at that database or schema
            """
            if len(self._valid_resource_types) == 0:
                raise Exception("Fully managed mode with all resources is not supported yet")
            
            for resource_type in self._valid_resource_types:
                # if resource_type not in (ResourceType.USER, ResourceType.ROLE, ResourceType.SCHEMA,):
                #     raise Exception("Fully managed mode with all resources is not supported yet")
                for name in data_provider.list_resource(session, resource_label_for_type(resource_type)):
                    urn = URN(resource_type=resource_type, fqn=parse_identifier(name, is_db_scoped=resource_type in (ResourceType.SCHEMA, ResourceType.ROLE)), account_locator=self._account_locator)
                    data = data_provider.fetch_resource(session, urn)
                    if data is not None:
                        normalized_data = _normalize(urn, data)
                        state[urn] = normalized_data

        for urn in manifest.keys():
            if str(urn).startswith("_"):
                continue

            manifest_urns.remove(urn)
            data = data_provider.fetch_resource(session, urn)
            if data is not None:
                normalized_data = _normalize(urn, data)
                state[urn] = normalized_data

        # for urn_str in manifest_urns:
        #     raise Exception(f"Resource {urn_str} not found in manifest")

        # check for existence of resource refs
        for parent, reference in manifest["_refs"]:
            if reference in manifest:
                continue
            
            
            try:
                data = data_provider.fetch_resource(session, reference)
            except Exception as e:
                data = None
            if data is None:
                logger.error(manifest)
                raise MissingResourceException(f"Resource {reference} required by {parent} not found or failed to fetch")

        return state

    def _finalize(self, session_context: dict):
        """
        Convert the staged resources into a tree of resources
        """
        if self._finalized:
            return
        self._finalized = True

        org_scoped, acct_scoped, db_scoped, schema_scoped = _split_by_scope(self._staged)
        self._staged = None

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
            # When the container is present, we SHOULD be looking up the real container and attaching. In other words
            # this resource could be an island
            if resource.container is None:
                if len(databases) == 1:
                    databases[0].add(resource)
                else:
                    raise Exception(f"Database [{resource.container}] for resource {resource} not found")
            else:
                for db in databases:
                    if db.name == resource.container.name and resource not in db:
                        db.add(resource)
                        break

        for resource in schema_scoped:
            if resource.container is None:
                if len(databases) == 1:
                    public_schema: Schema = databases[0].find(resource_type=ResourceType.SCHEMA, name="PUBLIC")
                    if public_schema:
                        public_schema.add(resource)
                else:
                    raise Exception(f"No schema for resource {repr(resource)} found")
            elif isinstance(resource.container, ResourcePointer):
                schema_pointer = resource.container
                
                # We have a schema-scoped resource (eg a view) that has a resource pointer for the schema. The job is to connect
                # that resource into the tree
                # 
                # If the schema pointer has no database, assume it lives in the only database we have
                if schema_pointer.container is None:
                    if len(databases) == 1:
                        databases[0].add(schema_pointer)
                    else:
                        raise Exception(f"No database for resource {resource} schema={resource.container}")
                elif isinstance(schema_pointer.container, ResourcePointer):
                    database_pointer = schema_pointer.container
                    found = False
                    for db in databases:
                        if db.name == database_pointer.name:
                            for schema in db.items(resource_type=ResourceType.SCHEMA):
                                if schema.name == schema_pointer.name:
                                    schema.add(resource)
                                    found = True
                                    break

                    if not found:
                        self._root.add(database_pointer)
                
        for database in databases:
            merge_schemas = []
            public_schema = None
            information_schema = None
            all_schemas = database.items(resource_type=ResourceType.SCHEMA)
            for schema in all_schemas:
                if schema.implicit:
                    if schema.name == "PUBLIC":
                        public_schema = schema
                    elif schema.name == "INFORMATION_SCHEMA":
                        information_schema = schema
                elif schema.name in ("PUBLIC", "INFORMATION_SCHEMA"):
                    merge_schemas.append(schema)
            
            for schema in merge_schemas:
                database.remove(schema)
                schema_items = schema.items()
                for item in schema_items:
                    schema.remove(item)
                    if schema.name == "PUBLIC":
                        public_schema.add(item)
                    elif schema.name == "INFORMATION_SCHEMA":
                        information_schema.add(item)

    def generate_manifest(self, session_context: dict = {}) -> Manifest:
        manifest: Manifest = {}
        refs = []
        urns = []

        self._finalize(session_context)

        for resource in _walk(self._root):
            if isinstance(resource, Resource):
                if resource.implicit and self._run_mode != RunMode.FULLY_MANAGED:
                    continue

            urn = URN(
                resource_type=resource.resource_type,
                fqn=resource.fqn,
                account_locator=self._account_locator,
            )

            data = resource.to_dict()

            if isinstance(resource, ResourcePointer):
                data["_pointer"] = True

            # manifest_key = str(urn)

            #### Special Cases
            if resource.resource_type == ResourceType.FUTURE_GRANT:
                # Role up FUTURE GRANTS on the same role/target to a single entry
                # TODO: support grant option, use a single character prefix on the priv
                if urn not in manifest:
                    manifest[urn] = {}
                on_type = data["on_type"].lower()
                if on_type not in manifest[urn]:
                    manifest[urn][on_type] = []
                if data["priv"] in manifest[urn][on_type]:
                    # raise Exception(f"Duplicate resource {urn} with conflicting data")
                    continue
                manifest[urn][on_type].append(data["priv"])

            #### Normal Case
            else:
                if urn in manifest:
                    if data != manifest[urn]:
                        # raise Exception(f"Duplicate resource {urn} with conflicting data")
                        continue
                manifest[urn] = data

            urns.append(urn)

            for ref in resource.refs:
                ref_urn = URN.from_resource(account_locator=self._account_locator, resource=ref)
                refs.append((urn, ref_urn))
        manifest["_refs"] = refs
        manifest["_urns"] = urns
        return manifest

    def plan(self, session) -> Plan:
        reset_cache()
        session_ctx = data_provider.fetch_session(session)
        manifest = self.generate_manifest(session_ctx)
        remote_state = self.fetch_remote_state(session, manifest)
        try:
            completed_plan = self._plan(remote_state, manifest)
        except Exception as e:
            logger.error("~" * 80 + "REMOTE STATE")
            logger.error(remote_state)
            logger.error("~" * 80 + "MANIFEST")
            logger.error(manifest)

            raise e
        self._raise_for_nonconforming_plan(completed_plan)
        return completed_plan

    def apply(self, session, plan: Plan = None):
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
    
        # required_privs = _collect_required_privs(session_ctx, plan)
        # available_privs = _collect_available_privs(session_ctx, session, plan, usable_roles)
        # _raise_if_missing_privs(required_privs, available_privs)

        action_queue = self._compile_plan_to_sql(session_ctx, plan)
        actions_taken = []

        while action_queue:
            sql = action_queue.pop(0)
            actions_taken.append(sql)
            try:
                if not self._dry_run:
                    execute(session, sql)
            except snowflake.connector.errors.ProgrammingError as err:
                if err.errno == ALREADY_EXISTS_ERR:
                    logger.error(f"Resource already exists: {sql}, skipping...")
                elif err.errno == INVALID_GRANT_ERR:
                    logger.error(f"Invalid grant: {sql}, skipping...")
                else:
                    raise err
        return actions_taken
    
    def _compile_plan_to_sql(self, session_ctx, plan: Plan):
        action_queue = []
        default_role = session_ctx["role"]
        # usable_roles = session_ctx["available_roles"] if self._allow_role_switching else [session_ctx["role"]]

        def _queue_change(change: ResourceChange, props):
            """
            In Snowflake's RBAC model, a session has an active role, and zero or more secondary roles.

            The active role of a session is set as follows:
            - When a session is started:
                - If the session is configured with a role, that is the active role
                - Otherwise, if the user of the session has a default_role set, and that role exists, that is the active role
                - Otherwise, the PUBLIC role is activated (PUBLIC cannot be revoked)
            - Any time the USE ROLE command is run, the active role is switched

            A session may run any command thats allowed by the active role or any role downstream from it in the role hierarchy.
            When secondary roles are active (by running the command USE SECONDARY ROLES ALL), then the session may also run any
            command that any secondary role or a role downstream from it is allowed to run.

            However, when a CREATE command is run, only the active role is considered. This is because the role that
            creates a new resource owns that resource by default. There are some exceptions with GRANTS.

            For those reasons, we generally don't have to worry about the current role as long as we have activated secondary roles.
            The exception is when creating new resources
            """
            
            before_action = []
            action = None
            after_action = []


            if change.action == Action.ADD:
                # TODO: raise exception if role isn't usable. Maybe can just let this fail naturally

                if "owner" in change.after:
                    if change.after["owner"] in SYSTEM_ROLES:
                        before_action.append(f"USE ROLE {change.after['owner']}")
                    else:
                        before_action.append(f"USE ROLE {default_role}")
                        after_action.append(f"GRANT OWNERSHIP ON {change.urn.resource_type} {change.urn.fqn} TO {change.after['owner']}")
                elif change.urn.resource_type in (ResourceType.FUTURE_GRANT, ResourceType.ROLE_GRANT):
                    # TODO: switch to role with MANAGE GRANTS if we dont have access to SECURITYADMIN
                    before_action.append(f"USE ROLE SECURITYADMIN")
                else:
                    before_action.append(f"USE ROLE {default_role}")
                
                action = lifecycle.create_resource(change.urn, change.after, props)
            elif change.action == Action.CHANGE:
                action = lifecycle.update_resource(change.urn, change.delta, props)
            elif change.action == Action.REMOVE:
                action = lifecycle.drop_resource(change.urn, change.before)

            action_queue.extend(before_action)
            action_queue.append(action)
            action_queue.extend(after_action)

        if self._allow_role_switching:
            action_queue.append("USE SECONDARY ROLES ALL")
        for change in plan:
            props = Resource.props_for_resource_type(change.urn.resource_type, change.after)
            _queue_change(change, props)
        return action_queue

    def destroy(self, session, manifest: Manifest=None):
        session_ctx = data_provider.fetch_session(session)
        manifest = manifest or self.generate_manifest(session_ctx)
        for urn, data in manifest.items():
            if str(urn).startswith("_"):
                continue

            if isinstance(data, dict) and data.get("_pointer"):
                continue
            if urn.resource_type == ResourceType.GRANT:
                for grant in data:
                    execute(session, lifecycle.drop_resource(urn, grant))
            else:
                try:
                    execute(session, lifecycle.drop_resource(urn, data))
                except snowflake.connector.errors.ProgrammingError as err:
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


def topological_sort(resource_set: set, references: set):
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
    if len(nodes) != len(resource_set):
        raise Exception("Graph is not a DAG")
    return {value: index for index, value in enumerate(nodes)}
