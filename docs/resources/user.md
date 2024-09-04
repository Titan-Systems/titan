---
description: >-
  
---

# User

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/create-user)

A user in Snowflake.


## Examples

### Python

```python
user = User(
    name="some_user",
    owner="USERADMIN",
    email="some.user@example.com",
    type="PERSON",
)
```


### YAML

```yaml
users:
  - name: some_user
    owner: USERADMIN
    email: some.user@example.com
    type: PERSON
```


## Fields

* `name` (string, required) - The name of the user.
* `owner` (string or [Role](role.md)) - The owner of the user. Defaults to "USERADMIN".
* `password` (string) - The password of the user.
* `login_name` (string) - The login name of the user. Defaults to the name in uppercase.
* `display_name` (string) - The display name of the user. Defaults to the name in lowercase.
* `first_name` (string) - The first name of the user.
* `middle_name` (string) - The middle name of the user.
* `last_name` (string) - The last name of the user.
* `email` (string) - The email of the user.
* `must_change_password` (bool) - Whether the user must change their password. Defaults to False.
* `disabled` (bool) - Whether the user is disabled. Defaults to False.
* `days_to_expiry` (int) - The number of days until the user's password expires.
* `mins_to_unlock` (int) - The number of minutes until the user's account is unlocked.
* `default_warehouse` (string) - The default warehouse for the user.
* `default_namespace` (string) - The default namespace for the user.
* `default_role` (string) - The default role for the user.
* `default_secondary_roles` (list) - The default secondary roles for the user.
* `mins_to_bypass_mfa` (int) - The number of minutes until the user can bypass Multi-Factor Authentication.
* `rsa_public_key` (string) - The RSA public key for the user.
* `rsa_public_key_2` (string) - The RSA public key for the user.
* `comment` (string) - A comment for the user.
* `network_policy` (string) - The network policy for the user.
* `type` (string or [UserType](user_type.md)) - The type of the user. Defaults to "NULL".
* `tags` (dict) - Tags for the user.


