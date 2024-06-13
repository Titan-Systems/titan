---
description: >-
  
---

# ObjectStoreCatalogIntegration

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/create-catalog-integration)

Manages the integration of an object store as a catalog in Snowflake, supporting the ICEBERG table format.


## Examples

### Python

```python
object_store_catalog_integration = ObjectStoreCatalogIntegration(
    name="some_catalog_integration",
    table_format="ICEBERG",
    enabled=True,
    comment="Integration for object storage."
)
```


### YAML

```yaml
catalog_integrations:
  - name: some_catalog_integration
    table_format: ICEBERG
    enabled: true
    comment: Integration for object storage.
```


## Fields

* `name` (string, required) - The name of the catalog integration.
* `table_format` (string or [CatalogTableFormat](catalog_table_format.md), required) - The format of the table, defaults to ICEBERG.
* `enabled` (bool) - Specifies whether the catalog integration is enabled. Defaults to True.
* `comment` (string) - An optional comment describing the catalog integration.


