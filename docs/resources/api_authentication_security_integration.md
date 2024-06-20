---
description: >-
  
---

# APIAuthenticationSecurityIntegration

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/create-security-integration)

Manages API authentication security integrations in Snowflake, allowing for secure API access management.


## Examples

### Python

```python
api_auth_integration = APIAuthenticationSecurityIntegration(
    name="some_api_authentication_security_integration",
    auth_type="OAUTH2",
    oauth_token_endpoint="https://example.com/oauth/token",
    oauth_client_auth_method="CLIENT_SECRET_POST",
    oauth_client_id="your_client_id",
    oauth_client_secret="your_client_secret",
    oauth_grant="client_credentials",
    oauth_access_token_validity=3600,
    oauth_allowed_scopes=["read", "write"],
    enabled=True,
    comment="Integration for external API authentication."
)
```


### YAML

```yaml
security_integrations:
- name: some_api_authentication_security_integration
    type: api_authentication
    auth_type: OAUTH2
    oauth_token_endpoint: https://example.com/oauth/token
    oauth_client_auth_method: CLIENT_SECRET_POST
    oauth_client_id: your_client_id
    oauth_client_secret: your_client_secret
    oauth_grant: client_credentials
    oauth_access_token_validity: 3600
    oauth_allowed_scopes: [read, write]
    enabled: true
    comment: Integration for external API authentication.
```


## Fields

* `name` (string, required) - The unique name of the security integration.
* `auth_type` (string) - The type of authentication used, typically 'OAUTH2'. Defaults to 'OAUTH2'.
* `oauth_token_endpoint` (string) - The endpoint URL for obtaining OAuth tokens.
* `oauth_client_auth_method` (string) - The method used for client authentication, such as 'CLIENT_SECRET_POST'.
* `oauth_client_id` (string) - The client identifier for OAuth.
* `oauth_client_secret` (string) - The client secret for OAuth.
* `oauth_grant` (string) - The OAuth grant type.
* `oauth_access_token_validity` (int) - The validity period of the OAuth access token in seconds. Defaults to 0.
* `oauth_allowed_scopes` (list) - A list of allowed scopes for the OAuth tokens.
* `enabled` (bool) - Indicates if the security integration is enabled. Defaults to True.
* `comment` (string) - An optional comment about the security integration.


