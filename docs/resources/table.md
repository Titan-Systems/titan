---
description: >-
  
---

# Table

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/create-table)

A table in Snowflake.


## Examples

### Python

```python
table = Table(
    name="some_table",
    columns=[{"name": "col1", "data_type": "STRING"}],
    owner="SYSADMIN",
)
```


### YAML

```yaml
tables:
  - name: some_table
    columns:
      - name: col1
        data_type: STRING
    owner: SYSADMIN
```


## Fields

* `name` (string, required) - The name of the table.
* `columns` (list, required) - The columns of the table.
* `constraints` (list) - The constraints of the table.
* `transient` (bool) - Whether the table is transient.
* `cluster_by` (list) - The clustering keys for the table.
* `enable_schema_evolution` (bool) - Whether schema evolution is enabled. Defaults to False.
* `data_retention_time_in_days` (int) - The data retention time in days.
* `max_data_extension_time_in_days` (int) - The maximum data extension time in days.
* `change_tracking` (bool) - Whether change tracking is enabled. Defaults to False.
* `default_ddl_collation` (string) - The default DDL collation.
* `copy_grants` (bool) - Whether to copy grants. Defaults to False.
* `row_access_policy` (dict) - The row access policy.
* `tags` (dict) - The tags for the table.
* `owner` (string or [Role](role.md)) - The owner role of the table. Defaults to SYSADMIN.
* `comment` (string) - A comment for the table.


