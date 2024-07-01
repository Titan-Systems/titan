---
description: >-
  
---

# FutureGrant

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/grant-privilege)

Represents a future grant of privileges on a resource to a role in Snowflake.


## Examples

### Python

```python
future_grant = FutureGrant(
    priv="SELECT",
    on_type="TABLE",
    in_type="SCHEMA",
    in_name="someschema",
    to="somerole",
)
```


### YAML

```yaml
future_grants:
  - priv: SELECT
    on_type: TABLE
    in_type: SCHEMA
    in_name: someschema
    to: somerole
```


## Fields

* `priv` (string, required) - The privilege to grant. Examples include 'SELECT', 'INSERT', 'CREATE TABLE'.
* `on_type` (string or [ResourceType](resource_type.md), required) - The type of resource on which the privilege is granted.
* `in_type` (string or [ResourceType](resource_type.md), required) - The type of container resource in which the privilege is granted.
* `in_name` (string, required) - The name of the container resource in which the privilege is granted.
* `to` (string or [Role](role.md), required) - The role to which the privileges are granted.
* `grant_option` (bool) - Specifies whether the grantee can grant the privileges to other roles. Defaults to False.


