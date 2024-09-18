# type: ignore

import yaml

from functools import cache

from titan.data_provider import list_schemas
from titan.enums import ResourceType
from titan.identifiers import FQN
from titan.privs import DatabasePriv, TablePriv, SchemaPriv, ViewPriv, WarehousePriv
from titan.resources import GrantOnAll, FutureGrant, Grant, RoleGrant
from titan.resources.resource import ResourcePointer


DATABASE_READ_PRIVS = [DatabasePriv.USAGE]
DATABASE_WRITE_PRIVS = [DatabasePriv.USAGE, DatabasePriv.MONITOR, DatabasePriv.CREATE_SCHEMA]

STORAGE_INTEGRATION_PRIVS = ["USAGE"]

SCHEMA_READ_PRIVS = [SchemaPriv.USAGE]
SCHEMA_WRITE_PRIVS = [
    SchemaPriv.USAGE,
    SchemaPriv.MONITOR,
    SchemaPriv.CREATE_ALERT,
    SchemaPriv.CREATE_DYNAMIC_TABLE,
    SchemaPriv.CREATE_EXTERNAL_TABLE,
    SchemaPriv.CREATE_FILE_FORMAT,
    SchemaPriv.CREATE_FUNCTION,
    # SchemaPriv.CREATE_MASKING_POLICY,
    # SchemaPriv.CREATE_MATERIALIZED_VIEW,
    # SchemaPriv.CREATE_NETWORK_RULE,
    # SchemaPriv.CREATE_PACKAGES_POLICY,
    # SchemaPriv.CREATE_PASSWORD_POLICY,
    SchemaPriv.CREATE_PIPE,
    SchemaPriv.CREATE_PROCEDURE,
    # SchemaPriv.CREATE_ROW_ACCESS_POLICY,
    SchemaPriv.CREATE_SECRET,
    SchemaPriv.CREATE_SEQUENCE,
    # SchemaPriv.CREATE_SESSION_POLICY,
    # SchemaPriv.CREATE_SNOWFLAKE_ML_ANOMALY_DETECTION,
    # SchemaPriv.CREATE_SNOWFLAKE_ML_FORECAST,
    SchemaPriv.CREATE_STAGE,
    SchemaPriv.CREATE_STREAM,
    SchemaPriv.CREATE_TABLE,
    # SchemaPriv.CREATE_TAG,
    SchemaPriv.CREATE_TASK,
    SchemaPriv.CREATE_VIEW,
]

TABLE_READ_PRIVS = [TablePriv.SELECT]
TABLE_WRITE_PRIVS = [
    TablePriv.SELECT,
    TablePriv.INSERT,
    TablePriv.UPDATE,
    TablePriv.DELETE,
    TablePriv.TRUNCATE,
    TablePriv.REFERENCES,
]

VIEW_READ_PRIVS = [ViewPriv.SELECT]
VIEW_WRITE_PRIVS = [ViewPriv.SELECT]

WAREHOUSE_PRIVS = [WarehousePriv.USAGE, WarehousePriv.OPERATE, WarehousePriv.MONITOR]


def _parse_permifrost_identifier(identifier: str, is_db_scoped: bool = False):
    """
    Parse a permifrost identifier into a database and schema.

    Args:
        identifier (str): The identifier to parse.
        is_db_scoped (bool): Whether the identifier is scoped to a database.

    Returns:
        tuple: A tuple containing the database and schema.
    """
    parts = identifier.split(".")
    if is_db_scoped:
        return FQN(database=parts[0], name=parts[1])
    return FQN(database=parts[0], schema=parts[1], name=parts[2])


@cache
def _list_schemas(session, database):
    return list_schemas(session, database)


def read_permifrost_config(session, file_path):
    """
    Read the permifrost config file and return the config as a dictionary.

    Args:
        file_path (str): The path to the permifrost config file.

    Returns:
        dict: The permifrost config as a dictionary.
    """
    # return resources.read_yaml(file_path)
    config = {}
    with open(file_path, "r") as file:
        config = yaml.safe_load(file)

    version = config.pop("version", None)
    databases = config.pop("databases", [])
    roles = config.pop("roles", [])
    users = config.pop("users", [])
    warehouses = config.pop("warehouses", [])
    integrations = config.pop("integrations", [])

    return [
        # *databases,
        *_get_role_resources(session, roles),
        *_get_user_resources(users),
        # warehouses,
        # integrations,
    ]


def _get_role_resources(session, roles: list):
    resources = []
    for permifrost_role in roles:
        role_name, config = permifrost_role.popitem()
        role = ResourcePointer(name=role_name, resource_type=ResourceType.ROLE)
        resources.append(role)

        warehouses = config.get("warehouses", [])
        for wh in warehouses:
            resources.append(ResourcePointer(name=wh, resource_type=ResourceType.WAREHOUSE))
            for priv in WAREHOUSE_PRIVS:
                resources.append(Grant(priv=priv, on_warehouse=wh, to=role))

        integrations = config.get("integrations", [])
        for integration in integrations:
            resources.append(ResourcePointer(name=integration, resource_type=ResourceType.STORAGE_INTEGRATION))
            for priv in STORAGE_INTEGRATION_PRIVS:
                resources.append(Grant(priv=priv, on_storage_integration=integration, to=role))

        member_of = config.get("member_of", [])
        for parent_role in member_of:
            if parent_role == "*":
                continue
            resources.append(RoleGrant(role=role, to_role=parent_role))

        database_read = config.get("privileges", {}).get("databases", {}).get("read", [])
        database_write = config.get("privileges", {}).get("databases", {}).get("write", [])

        def _add_database_grants(resources, databases, privs, role):
            for db in databases:
                if db.upper() == "SNOWFLAKE":
                    continue
                resources.append(ResourcePointer(name=db, resource_type=ResourceType.DATABASE))
                for priv in privs:
                    resources.append(Grant(priv=priv, on_database=db, to=role))

        _add_database_grants(resources, database_read, DATABASE_READ_PRIVS, role)
        _add_database_grants(resources, database_write, DATABASE_WRITE_PRIVS, role)

        schema_read = config.get("privileges", {}).get("schemas", {}).get("read", [])
        schema_write = config.get("privileges", {}).get("schemas", {}).get("write", [])

        def _add_schema_grants(resources, schema_identifier, privs, role):
            if schema_identifier.endswith(".*"):
                database = _parse_permifrost_identifier(schema_identifier, is_db_scoped=True).database
                for schema in _list_schemas(session, database):
                    if schema.endswith("INFORMATION_SCHEMA"):
                        continue
                    for priv in privs:
                        resources.append(Grant(priv=priv, on_schema=schema, to=role))
                for priv in privs:
                    resources.append(FutureGrant(priv=priv, on_future_schemas_in_database=database, to=role))
            elif schema_identifier.endswith("*"):
                # schema: "db.schema_*"
                return
            else:
                fqn = _parse_permifrost_identifier(schema_identifier, is_db_scoped=True)
                if fqn.database.upper() == "SNOWFLAKE":
                    return
                db = ResourcePointer(name=fqn.database, resource_type=ResourceType.DATABASE)
                schema = ResourcePointer(name=fqn.name, resource_type=ResourceType.SCHEMA)
                db.add(schema)
                resources.append(db)
                resources.append(schema)
                for priv in privs:
                    resources.append(Grant(priv=priv, on_schema=schema_identifier, to=role))

        for schema in schema_read:
            _add_schema_grants(resources, schema, SCHEMA_READ_PRIVS, role)
        for schema in schema_write:
            _add_schema_grants(resources, schema, SCHEMA_WRITE_PRIVS, role)

        table_read = config.get("privileges", {}).get("tables", {}).get("read", [])
        table_write = config.get("privileges", {}).get("tables", {}).get("write", [])

        def _add_table_grants(resources, table_identifier, privs, role):
            if table_identifier.endswith(".*.*"):
                database = _parse_permifrost_identifier(table_identifier).database
                for schema in _list_schemas(session, database):
                    if schema.endswith("INFORMATION_SCHEMA"):
                        continue
                    for priv in privs:
                        resources.append(GrantOnAll(priv=priv, on_all_tables_in_schema=schema, to=role))
                        resources.append(FutureGrant(priv=priv, on_future_tables_in_schema=schema, to=role))

                for priv in privs:
                    resources.append(GrantOnAll(priv=priv, on_all_tables_in_database=database, to=role))
                    resources.append(FutureGrant(priv=priv, on_future_tables_in_database=database, to=role))
            elif table_identifier.endswith(".*"):
                fqn = _parse_permifrost_identifier(table_identifier)
                if fqn.schema.endswith("*") or fqn.database.upper() == "SNOWFLAKE":
                    return
                db = ResourcePointer(name=fqn.database, resource_type=ResourceType.DATABASE)
                schema = ResourcePointer(name=fqn.schema, resource_type=ResourceType.SCHEMA)
                db.add(schema)
                resources.append(db)
                resources.append(schema)
                for priv in privs:
                    resources.append(GrantOnAll(priv=priv, on_all_tables_in=schema, to=role))
                    resources.append(FutureGrant(priv=priv, on_future_tables_in=schema, to=role))
            elif table_identifier.endswith("*"):
                # table: "db.schema.table_*"
                return
            else:
                resources.append(ResourcePointer(name=table_identifier, resource_type=ResourceType.TABLE))
                for priv in privs:
                    resources.append(Grant(priv=priv, on_table=table_identifier, to=role))

        def _add_view_grants(resources, view_identifier, privs, role):
            if view_identifier.endswith(".*.*"):
                database = _parse_permifrost_identifier(view_identifier).database
                for schema in _list_schemas(session, database):
                    if schema.endswith("INFORMATION_SCHEMA"):
                        continue
                    for priv in privs:
                        resources.append(GrantOnAll(priv=priv, on_all_views_in_schema=schema, to=role))
                        resources.append(FutureGrant(priv=priv, on_future_views_in_schema=schema, to=role))
                for priv in privs:
                    resources.append(GrantOnAll(priv=priv, on_all_views_in_database=database, to=role))
                    resources.append(FutureGrant(priv=priv, on_future_views_in_database=database, to=role))
            elif view_identifier.endswith(".*"):
                fqn = _parse_permifrost_identifier(view_identifier)
                if fqn.schema.endswith("*") or fqn.database.upper() == "SNOWFLAKE":
                    return
                db = ResourcePointer(name=fqn.database, resource_type=ResourceType.DATABASE)
                schema = ResourcePointer(name=fqn.schema, resource_type=ResourceType.SCHEMA)
                db.add(schema)
                resources.append(db)
                resources.append(schema)
                for priv in privs:
                    resources.append(GrantOnAll(priv=priv, on_all_views_in=schema, to=role))
                    resources.append(FutureGrant(priv=priv, on_future_views_in=schema, to=role))

            elif view_identifier.endswith("*"):
                # table: "db.schema.table_*"
                return
            else:
                resources.append(ResourcePointer(name=view_identifier, resource_type=ResourceType.VIEW))
                for priv in privs:
                    resources.append(Grant(priv=priv, on_view=view_identifier, to=role))

        for table in table_read:
            _add_table_grants(resources, table, TABLE_READ_PRIVS, role)
            _add_view_grants(resources, table, VIEW_READ_PRIVS, role)
        for table in table_write:
            _add_table_grants(resources, table, TABLE_WRITE_PRIVS, role)
            _add_view_grants(resources, table, VIEW_WRITE_PRIVS, role)

        # TODO: owns

    return resources


def _get_user_resources(users: list):
    resources = []
    for permifrost_user in users:
        user, config = permifrost_user.popitem()
        resources.append(ResourcePointer(name=user, resource_type=ResourceType.USER))

        member_of = config.get("member_of", [])
        for role in member_of:
            if role == "*":
                continue
            resources.append(RoleGrant(role=role, to_user=user))

    return resources
