---
description: >-
  
---

# DatabaseRole

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/create-database-role)

A database role in Snowflake is a collection of privileges that can be assigned to users or other roles within a specific database context. It is used to manage access control and permissions at the database level.


## Examples

### Python

```python
database_role = DatabaseRole(
    name="some_database_role",
    database="some_database",
    owner="USERADMIN",
    tags={"department": "finance"},
    comment="This role is for database-specific access control."
)
```


### YAML

```yaml
database_roles:
  - name: some_database_role
    database: some_database
    owner: USERADMIN
    tags:
      department: finance
    comment: This role is for database-specific access control.
```


## Fields

* `name` (string, required) - The name of the database role.
* `database` (string) - The database this role is associated with. This is derived from the fully qualified name.
* `owner` (string) - The owner of the database role. Defaults to "USERADMIN".
* `tags` (dict) - Tags associated with the database role.
* `comment` (string) - A comment about the database role.


