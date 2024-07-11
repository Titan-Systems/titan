---
description: >-
  
---

# FutureGrant

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/grant-privilege)

Represents a future grant of privileges on a resource to a role in Snowflake.


## Examples

### Python

```python
# Database Object Privs:
future_grant = FutureGrant(
    priv="CREATE TABLE",
    on_future_schemas_in=Database(name="somedb"),
    to="somerole",
)
future_grant = FutureGrant(
    priv="CREATE TABLE",
    on_future_schemas_in_database="somedb",
    to="somerole",
)
# Schema Object Privs:
future_grant = FutureGrant(
    priv="SELECT",
    on_future_tables_in=Schema(name="someschema"),
    to="somerole",
)
future_grant = FutureGrant(
    priv="READ",
    on_future_image_repositories_in_schema="someschema",
    to="somerole",
)
```


### YAML

```yaml
future_grants:
  - priv: SELECT
    on_future_tables_in_schema: someschema
    to: somerole
```


## Fields

* `priv` (string, required) - The privilege to grant. Examples include 'SELECT', 'INSERT', 'CREATE TABLE'.
* `on_type` (string or [ResourceType](resource_type.md), required) - The type of resource on which the privilege is granted.
* `in_type` (string or [ResourceType](resource_type.md), required) - The type of container resource in which the privilege is granted.
* `in_name` (string, required) - The name of the container resource in which the privilege is granted.
* `to` (string or [Role](role.md), required) - The role to which the privileges are granted.
* `grant_option` (bool) - Specifies whether the grantee can grant the privileges to other roles. Defaults to False.


