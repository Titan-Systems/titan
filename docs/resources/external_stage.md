---
description: >-
  
---

# ExternalStage

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/create-stage)

Manages external stages in Snowflake, which are used to reference external storage locations.


## Examples

### Python

```python
external_stage = ExternalStage(
    name="some_external_stage",
    url="https://example.com/storage",
    owner="SYSADMIN",
    storage_integration="some_integration"
)
```


### YAML

```yaml
stages:
  - name: some_external_stage
    type: external
    url: https://example.com/storage
    owner: SYSADMIN
    storage_integration: some_integration
```


## Fields

* `name` (string, required) - The name of the external stage.
* `url` (string, required) - The URL pointing to the external storage location.
* `owner` (string or [Role](role.md)) - The owner role of the external stage. Defaults to "SYSADMIN".
* `storage_integration` (string) - The name of the storage integration used with this stage.
* `credentials` (dict) - The credentials for accessing the external storage, if required.
* `encryption` (dict) - The encryption settings used for data stored in the external location.
* `directory` (dict) - Settings related to directory handling in the external storage.
* `tags` (dict) - Tags associated with the external stage.
* `comment` (string) - A comment about the external stage.


