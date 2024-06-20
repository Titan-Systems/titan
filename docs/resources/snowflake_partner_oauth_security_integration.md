---
description: >-
  
---

# SnowflakePartnerOAuthSecurityIntegration

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/create-security-integration)

A security integration in Snowflake designed to manage external OAuth clients for authentication purposes.
This integration supports specific OAuth clients such as Looker, Tableau Desktop, and Tableau Server.


## Examples

### Python

```python
snowflake_partner_oauth_security_integration = SnowflakePartnerOAuthSecurityIntegration(
    name="some_security_integration",
    enabled=True,
    oauth_client="LOOKER",
    oauth_client_secret="secret123",
    oauth_redirect_uri="https://example.com/oauth/callback",
    oauth_issue_refresh_tokens=True,
    oauth_refresh_token_validity=7776000,
    comment="Integration for Looker OAuth"
)
```


### YAML

```yaml
security_integrations:
  - name: some_security_integration
    enabled: true
    oauth_client: LOOKER
    oauth_client_secret: secret123
    oauth_redirect_uri: https://example.com/oauth/callback
    oauth_issue_refresh_tokens: true
    oauth_refresh_token_validity: 7776000
    comment: Integration for Looker OAuth
```


## Fields

* `name` (string, required) - The name of the security integration.
* `enabled` (bool) - Specifies if the security integration is enabled. Defaults to True.
* `oauth_client` (string or [OAuthClient](oauth_client.md)) - The OAuth client used for authentication. Supported clients are 'LOOKER', 'TABLEAU_DESKTOP', and 'TABLEAU_SERVER'.
* `oauth_client_secret` (string) - The secret associated with the OAuth client.
* `oauth_redirect_uri` (string) - The redirect URI configured for the OAuth client.
* `oauth_issue_refresh_tokens` (bool) - Indicates if refresh tokens should be issued. Defaults to True.
* `oauth_refresh_token_validity` (int) - The validity period of the refresh token in seconds.
* `comment` (string) - A comment about the security integration.


