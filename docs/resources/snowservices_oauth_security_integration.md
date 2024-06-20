---
description: >-
  
---

# SnowservicesOAuthSecurityIntegration

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/create-security-integration)

Manages OAuth security integrations for Snowservices in Snowflake, allowing external authentication mechanisms.


## Examples

### Python

```python
snowservices_oauth = SnowservicesOAuthSecurityIntegration(
    name="some_security_integration",
    enabled=True,
    comment="Integration for external OAuth services."
)
```


### YAML

```yaml
snowservices_oauth:
  - name: some_security_integration
    enabled: true
    comment: Integration for external OAuth services.
```


## Fields

* `name` (string, required) - The name of the security integration.
* `enabled` (bool) - Specifies if the security integration is enabled. Defaults to True.
* `comment` (string) - A comment about the security integration.


