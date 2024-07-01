---
description: >-
  
---

# RoleGrant

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/grant-role)

Represents a grant of a role to another role or user in Snowflake.


## Examples

### Python

```python
# Grant to Role:
role_grant = RoleGrant(role="somerole", to_role="someotherrole")
role_grant = RoleGrant(role="somerole", to=Role(name="someotherrole"))
# Grant to User:
role_grant = RoleGrant(role="somerole", to_user="someuser")
role_grant = RoleGrant(role="somerole", to=User(name="someuser"))
```


### YAML

```yaml
role_grants:
  - role: somerole
    to_role: someotherrole
  - role: somerole
    to_user: someuser
```


## Fields

* `role` (string or [Role](role.md), required) - The role to be granted.
* `to_role` (string or [Role](role.md)) - The role to which the role is granted.
* `to_user` (string or [User](user.md)) - The user to which the role is granted.


