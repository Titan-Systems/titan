---
description: >-
  
---

# GenericSecret

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/create-secret)

A Secret defines a set of sensitive data that can be used for authentication or other purposes.
This class defines a generic secret.


## Examples

### Python

```python
secret = GenericSecret(
    name="some_secret",
    secret_string="some_secret_string",
    comment="some_comment",
    owner="SYSADMIN",
)
```


### YAML

```yaml
secrets:
  - name: some_secret
    secret_type: GENERIC_STRING
    secret_string: some_secret_string
    comment: some_comment
    owner: SYSADMIN
```


## Fields

* `name` (string, required) - The name of the secret.
* `secret_string` (string) - The secret string.
* `comment` (string) - A comment for the secret.
* `owner` (string or [Role](role.md)) - The owner of the secret. Defaults to SYSADMIN.


