---
description: >-
  
---

# GCPAPIIntegration

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/create-api-integration)

Manages API integrations in Snowflake, allowing external services to interact with Snowflake resources securely.
This class supports creating, replacing, and checking the existence of API integrations with various configurations.


## Examples

### Python

```python
api_integration = APIIntegration(
    name="some_api_integration",
    api_provider="GOOGLE_API_GATEWAU",
    google_audience="<google_audience>",
    enabled=True,
    api_allowed_prefixes=["https://some_url.com"],
    comment="Example GCP API integration"
)
```


### YAML

```yaml
api_integrations:
  - name: some_api_integration
    api_provider: GOOGLE_API_GATEWAY
    google_audience: <google_audience>
    enabled: true
    api_allowed_prefixes:
        - https://some_url.com
    comment: "Example GCP API integration"
```


## Fields

* `name` (string, required) - The unique name of the API integration.
* `api_provider` (string or [ApiProvider](api_provider.md), required) - The provider of the API service.
* `google_audience` (string, required) - The audience claim when generating the JWT to authenticate with the Google API Gateway.
* `api_allowed_prefixes` (list, required) - The list of allowed prefixes for the API endpoints.
* `api_blocked_prefixes` (list) - The list of blocked prefixes for the API endpoints.
* `enabled` (bool, required) - Specifies if the API integration is enabled. Defaults to TRUE.
* `comment` (string) - A comment or description for the API integration.


