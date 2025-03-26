---
description: >-
  
---

# GitAPIIntegration

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/create-api-integration)

Manages API integrations in Snowflake, allowing external services to interact with Snowflake resources securely.
This class supports creating, replacing, and checking the existence of API integrations with various configurations.


## Examples

### Python

```python
api_integration = APIIntegration(
    name="some_api_integration",
    api_provider="GIT_HTTPS_API",
    enabled=True,
    api_allowed_prefixes=["https://github.com/<org-name>"],
    comment="Example Git API integration"
)
```


### YAML

```yaml
api_integrations:
  - name: some_api_integration
    api_provider: GIT_HTTPS_API
    enabled: true
    api_allowed_prefixes:
        - https://github.com/<org-name>
    comment: "Example Git API integration"
```


## Fields

* `name` (string, required) - The unique name of the API integration.
* `api_provider` (string or [ApiProvider](api_provider.md), required) - The provider of the API service.
* `api_allowed_prefixes` (list, required) - The list of allowed prefixes for the API endpoints.
* `api_blocked_prefixes` (list) - The list of blocked prefixes for the API endpoints.
* `enabled` (bool, required) - Specifies if the API integration is enabled. Defaults to TRUE.
* `comment` (string) - A comment or description for the API integration.


