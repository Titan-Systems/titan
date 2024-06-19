---
description: >-
  
---

# Grant

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/grant-privilege)

Represents a grant of privileges on a resource to a role in Snowflake.


## Examples

### Python

```python
grant = Grant(
    priv="SELECT",
    on="some_table",
    to="some_role",
    grant_option=True
)
```


### YAML

```yaml
- Grant:
    priv: "SELECT"
    on: "some_table"
    to: "some_role"
    grant_option: true
```


## Fields

* `priv` (string, required) - The privilege to grant. Examples include 'SELECT', 'INSERT', 'CREATE TABLE'.
* `on` (string or [Resource](resource.md), required) - The resource on which the privilege is granted. Can be a string like 'ACCOUNT' or a specific resource object.
* `to` (string or [Role](role.md), required) - The role to which the privileges are granted.
* `grant_option` (bool) - Specifies whether the grantee can grant the privileges to other roles. Defaults to False.
* `owner` (string or [Role](role.md)) - The owner role of the grant. Defaults to 'SYSADMIN'.


