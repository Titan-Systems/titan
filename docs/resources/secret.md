---
description: >-
  
---

# Secret

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/create-secret)

A Secret defines a set of sensitive data that can be used for authentication or other purposes.


## Examples

### Python

```python
secret = Secret(
    name="some_secret",
    type="OAUTH2",
    api_authentication="some_security_integration",
    oauth_scopes=["scope1", "scope2"],
    oauth_refresh_token="some_refresh_token",
    oauth_refresh_token_expiry_time="some_expiry_time",
    username="some_username",
    password="some_password",
    secret_string="some_secret_string",
    comment="some_comment",
    owner="SYSADMIN",
)
```


### YAML

```yaml
secrets:
  - name: some_secret
    type: OAUTH2
    api_authentication: some_security_integration
    oauth_scopes:
      - scope1
      - scope2
    oauth_refresh_token: some_refresh_token
    oauth_refresh_token_expiry_time: some_expiry_time
    username: some_username
    password: some_password
    secret_string: some_secret_string
    comment: some_comment
    owner: SYSADMIN
```


## Fields

* `name` (string, required) - The name of the secret.
* `type` (string or [SecretType](secret_type.md), required) - The type of the secret.
* `api_authentication` (string) - The security integration name for API authentication.
* `oauth_scopes` (list) - The OAuth scopes for the secret.
* `oauth_refresh_token` (string) - The OAuth refresh token.
* `oauth_refresh_token_expiry_time` (string) - The expiry time of the OAuth refresh token.
* `username` (string) - The username for the secret.
* `password` (string) - The password for the secret.
* `secret_string` (string) - The secret string.
* `comment` (string) - A comment for the secret.
* `owner` (string or [Role](role.md)) - The owner of the secret. Defaults to SYSADMIN.


