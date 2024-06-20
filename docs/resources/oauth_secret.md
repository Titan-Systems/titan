---
description: >-
  
---

# OAuthSecret

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/create-secret)

A Secret defines a set of sensitive data that can be used for authentication or other purposes.
This class defines an OAuth secret.


## Examples

### Python

```python
# OAuth with client credentials flow:
secret = OAuthSecret(
    name="some_secret",
    api_authentication="some_security_integration",
    oauth_scopes=["scope1", "scope2"],
    comment="some_comment",
    owner="SYSADMIN",
)
# OAuth with authorization code grant flow:
secret = OAuthSecret(
    name="another_secret",
    api_authentication="some_security_integration",
    oauth_refresh_token="34n;vods4nQsdg09wee4qnfvadH",
    oauth_refresh_token_expiry_time="2049-01-06 20:00:00",
    comment="some_comment",
    owner="SYSADMIN",
)
```


### YAML

```yaml
secrets:
  - name: some_secret
    secret_type: OAUTH2
    api_authentication: some_security_integration
    oauth_scopes:
      - scope1
      - scope2
    comment: some_comment
    owner: SYSADMIN
  - name: another_secret
    secret_type: OAUTH2
    api_authentication: some_security_integration
    oauth_refresh_token: 34n;vods4nQsdg09wee4qnfvadH
    oauth_refresh_token_expiry_time: 2049-01-06 20:00:00
    comment: some_comment
    owner: SYSADMIN
```


## Fields

* `name` (string, required) - The name of the secret.
* `api_authentication` (string) - The security integration name for API authentication.
* `oauth_scopes` (list) - The OAuth scopes for the secret.
* `oauth_refresh_token` (string) - The OAuth refresh token.
* `oauth_refresh_token_expiry_time` (string) - The expiry time of the OAuth refresh token.
* `comment` (string) - A comment for the secret.
* `owner` (string or [Role](role.md)) - The owner of the secret. Defaults to SYSADMIN.


