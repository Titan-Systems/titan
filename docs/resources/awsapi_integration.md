---
description: >-
  
---

# AWSAPIIntegration

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/create-api-integration)

Manages API integrations in Snowflake, allowing external services to interact with Snowflake resources securely.
This class supports creating, replacing, and checking the existence of API integrations with various configurations.


## Examples

### Python

```python
api_integration = APIIntegration(
    name="some_api_integration",
    api_provider="AWS_API_GATEWAY",
    api_aws_role_arn="arn:aws:iam::123456789012:role/MyRole",
    enabled=True,
    api_allowed_prefixes=["/prod/", "/dev/"],
    api_blocked_prefixes=["/test/"],
    api_key="ABCD1234",
    comment="Example API integration"
)
```


### YAML

```yaml
api_integrations:
  - name: some_api_integration
    api_provider: AWS_API_GATEWAY
    api_aws_role_arn: "arn:aws:iam::123456789012:role/MyRole"
    enabled: true
    api_allowed_prefixes: ["/prod/", "/dev/"]
    api_blocked_prefixes: ["/test/"]
    api_key: "ABCD1234"
    comment: "Example API integration"
```


## Fields

* `name` (string, required) - The unique name of the API integration.
* `api_provider` (string or [ApiProvider](api_provider.md), required) - The provider of the API service. Defaults to AWS_API_GATEway.
* `api_aws_role_arn` (string, required) - The AWS IAM role ARN associated with the API integration.
* `api_key` (string) - The API key used for authentication.
* `api_allowed_prefixes` (list) - The list of allowed prefixes for the API endpoints.
* `api_blocked_prefixes` (list) - The list of blocked prefixes for the API endpoints.
* `enabled` (bool, required) - Specifies if the API integration is enabled. Defaults to TRUE.
* `comment` (string) - A comment or description for the API integration.


