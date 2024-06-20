---
description: >-
  
---

# GCSStorageIntegration

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/create-storage-integration)

Manages the integration of Google Cloud Storage (GCS) as an external stage for storing data.


## Examples

### Python

```python
gcs_storage_integration = GCSStorageIntegration(
    name="some_gcs_storage_integration",
    enabled=True,
    storage_allowed_locations=['gcs://bucket/path/'],
    storage_blocked_locations=['gcs://bucket/blocked_path/']
)
```


### YAML

```yaml
gcs_storage_integrations:
  - name: some_gcs_storage_integration
    enabled: true
    storage_allowed_locations:
      - 'gcs://bucket/path/'
    storage_blocked_locations:
      - 'gcs://bucket/blocked_path/'
```


## Fields

* `name` (string, required) - The name of the storage integration.
* `enabled` (bool, required) - Specifies whether the storage integration is enabled.
* `storage_allowed_locations` (list) - A list of allowed GCS locations for data storage.
* `storage_blocked_locations` (list) - A list of blocked GCS locations for data storage.
* `owner` (string or [Role](role.md)) - The owner role of the storage integration. Defaults to 'ACCOUNTADMIN'.
* `comment` (string) - An optional comment about the storage integration.


