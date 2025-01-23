---
description: >-
  
---

# DatabaseRoleGrant

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/grant-database-role)

Represents a grant of a database role to another role or database role in Snowflake.


## Examples

### Python

```python
# Grant to Database Role:
role_grant = DatabaseRoleGrant(database_role="somedb.somerole", to_database_role="somedb.someotherrole")
role_grant = DatabaseRoleGrant(database_role="somedb.somerole", to=DatabaseRole(database="somedb", name="someotherrole"))
# Grant to Role:
role_grant = DatabaseRoleGrant(database_role="somedb.somerole", to_role="somerole")
role_grant = DatabaseRoleGrant(database_role="somedb.somerole", to=Role(name="somerole"))
```


### YAML

```yaml
database_role_grants:
  - database_role: somedb.somerole
    to_database_role: somedb.someotherrole
  - database_role: somedb.somerole
    to_role: somerole
```


## Fields

* `database_role` (string or [Role](role.md), required) - The database role to be granted.
* `to_role` (string or [Role](role.md)) - The role to which the database role is granted.
* `to_database_role` (string or [User](user.md)) - The database role to which the database role is granted.


