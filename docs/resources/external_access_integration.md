---
description: >-
  
---

# ExternalAccessIntegration

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/create-external-access-integration)

External Access Integrations enable code within functions and stored procedures to utilize secrets and establish connections with external networks. This resource configures the rules and secrets that can be accessed by such code.


## Examples

### Python

```python
external_access_integration = ExternalAccessIntegration(
    name="some_external_access_integration",
    allowed_network_rules=["rule1", "rule2"],
    enabled=True
)
```


### YAML

```yaml
external_access_integrations:
  - name: some_external_access_integration
    allowed_network_rules:
      - rule1
      - rule2
    enabled: true
```


## Fields

* `name` (string, required) - The name of the external access integration.
* `allowed_network_rules` (list, required) - [NetworkRules](network_rule.md) that are allowed for this integration.
* `allowed_api_authentication_integrations` (list) - API authentication integrations that are allowed.
* `allowed_authentication_secrets` (list) - Authentication secrets that are allowed.
* `enabled` (bool) - Specifies if the integration is enabled. Defaults to True.
* `comment` (string) - An optional comment about the integration.
* `owner` (string or [Role](role.md)) - The owner role of the external access integration. Defaults to "ACCOUNTADMIN".


