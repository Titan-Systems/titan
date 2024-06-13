---
description: >-
  
---

# AzureStorageIntegration

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/create-storage-integration)

Represents an Azure storage integration in Snowflake, which allows Snowflake to access external cloud storage using Azure credentials.


## Examples

### Python

```python
azure_storage_integration = AzureStorageIntegration(
    name="some_azure_storage_integration",
    enabled=True,
    azure_tenant_id="some_tenant_id",
    storage_allowed_locations=["azure://somebucket/somepath/"],
    storage_blocked_locations=["azure://someotherbucket/somepath/"],
    comment="This is an Azure storage integration."
)
```


### YAML

```yaml
azure_storage_integrations:
  - name: some_azure_storage_integration
    enabled: true
    azure_tenant_id: some_tenant_id
    storage_allowed_locations:
      - azure://somebucket/somepath/
    storage_blocked_locations:
      - azure://someotherbucket/somepath/
    comment: This is an Azure storage integration.
```


## Fields

* `name` (string, required) - The name of the storage integration.
* `enabled` (bool, required) - Specifies whether the storage integration is enabled.
* `azure_tenant_id` (string, required) - The Azure tenant ID associated with the storage integration.
* `storage_allowed_locations` (list) - The cloud storage locations that are allowed.
* `storage_blocked_locations` (list) - The cloud storage locations that are blocked.
* `owner` (string or [Role](role.md)) - The owner role of the storage integration. Defaults to "ACCOUNTADMIN".
* `comment` (string) - A comment about the storage integration.


