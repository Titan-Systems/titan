from .resources import (
    Database,
    Role,
    RoleGrant,
    Schema,
    User,
    Warehouse,
)


def role_grants_from_config(role_grants_config: list) -> list:
    role_grants = []
    for role_grant in role_grants_config:
        for user in role_grant.get("users", []):
            role_grants.append(
                RoleGrant(
                    role=role_grant["role"],
                    to_user=user,
                )
            )
        for to_role in role_grant.get("roles", []):
            role_grants.append(
                RoleGrant(
                    role=role_grant["role"],
                    to_role=to_role,
                )
            )
    return role_grants


def databases_from_config(databases_config: list) -> list:
    databases = []
    for database in databases_config:
        schemas = database.pop("schemas", [])
        db = Database(**database)
        for schema in schemas:
            db.add(Schema(**schema))
        databases.append(db)
    return databases


def collect_resources_from_config(config: dict):
    config = config.copy()
    databases = config.pop("databases", [])
    role_grants = config.pop("role_grants", [])
    roles = config.pop("roles", [])
    users = config.pop("users", [])
    warehouses = config.pop("warehouses", [])

    if config:
        raise ValueError(f"Unknown keys in config: {config.keys()}")

    databases = databases_from_config(databases)
    role_grants = role_grants_from_config(role_grants)
    roles = [Role(**role) for role in roles]
    users = [User(**user) for user in users]
    warehouses = [Warehouse(**warehouse) for warehouse in warehouses]

    return (
        *databases,
        *role_grants,
        *roles,
        *users,
        *warehouses,
    )
