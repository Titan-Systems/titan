---
description: >-
  
---

# Role

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/create-role)

A role in Snowflake defines a set of access controls and permissions.


## Examples

### Python

```python
role = Role(
    name="some_role",
    owner="USERADMIN",
    comment="This is a sample role.",
)
```


### YAML

```yaml
roles:
  - name: some_role
    owner: USERADMIN
    comment: This is a sample role.
```


## Fields

* `name` (string, required) - The name of the role.
* `owner` (string) - The owner of the role. Defaults to "USERADMIN".
* `tags` (dict) - Tags associated with the role.
* `comment` (string) - A comment for the role.


