from .resources import Database, Warehouse, Role, User, RoleGrant


def role_grants_from_config(role_grants: list) -> list:
    role_grants = []
    for role_grant in role_grants:
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


def collect_resources_from_config(config: dict):
    databases = config.get("databases", [])
    role_grants = config.get("role_grants", [])
    roles = config.get("roles", [])
    users = config.get("users", [])
    warehouses = config.get("warehouses", [])

    databases = [Database(**database) for database in databases]
    users = [User(**user) for user in users]
    roles = [Role(**role) for role in roles]
    role_grants = role_grants_from_config(role_grants)
    warehouses = [Warehouse(**warehouse) for warehouse in warehouses]

    return (
        *databases,
        *role_grants,
        *roles,
        *users,
        *warehouses,
    )
