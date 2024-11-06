---
description: >-
  
---

# GrantOnAll

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/grant-privilege)

Represents a grant of privileges on all resources of a specified type to a role in Snowflake.


## Examples

### Python

```python
    # Schema Privs:
    grant_on_all = GrantOnAll(
        priv="CREATE TABLE",
        on_all_schemas_in_database="somedb",
        to="somerole",
    )
    grant_on_all = GrantOnAll(
        priv="CREATE VIEW",
        on_all_schemas_in=Database(name="somedb"),
        to="somerole",
    )
    # Schema Object Privs:
    grant_on_all = GrantOnAll(
        priv="SELECT",
        on_all_tables_in_schema="someschema",
        to="somerole",
    )
    grant_on_all = GrantOnAll(
        priv="SELECT",
        on_all_views_in_database="somedb",
        to="somerole",
    )
```


### YAML

```yaml
grants_on_all:
    - priv: SELECT
        on_all_tables_in_schema: someschema
        to: somerole
```


## Fields

* `priv` (string, required) - The privilege to grant. Examples include 'SELECT', 'INSERT', 'CREATE TABLE'.
* `on_type` (string or [ResourceType](resource_type.md), required) - The type of resource on which the privileges are granted.
* `in_type` (string or [ResourceType](resource_type.md), required) - The type of container resource in which the privilege is granted.
* `in_name` (string, required) - The name of the container resource in which the privilege is granted.
* `to` (string or [Role](role.md), required) - The role to which the privileges are granted.
* `grant_option` (bool) - Specifies whether the grantee can grant the privileges to other roles. Defaults to False.


