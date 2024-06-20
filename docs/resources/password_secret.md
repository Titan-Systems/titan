---
description: >-
  
---

# PasswordSecret

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/create-secret)

A Secret defines a set of sensitive data that can be used for authentication or other purposes.
This class defines a password secret.


## Examples

### Python

```python
secret = PasswordSecret(
    name="some_secret",
    username="some_username",
    password="some_password",
    comment="some_comment",
    owner="SYSADMIN",
)
```


### YAML

```yaml
secrets:
  - name: some_secret
    secret_type: PASSWORD
    username: some_username
    password: some_password
    comment: some_comment
    owner: SYSADMIN
```


## Fields

* `name` (string, required) - The name of the secret.
* `username` (string) - The username for the secret.
* `password` (string) - The password for the secret.
* `comment` (string) - A comment for the secret.
* `owner` (string or [Role](role.md)) - The owner of the secret. Defaults to SYSADMIN.


