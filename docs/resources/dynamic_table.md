---
description: >-
  
---

# DynamicTable

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/create-dynamic-table)

Represents a dynamic table in Snowflake, which can be configured to refresh automatically,
fully, or incrementally, and initialized on creation or on a schedule.


## Examples

### Python

```python
dynamic_table = DynamicTable(
    name="some_dynamic_table",
    columns=[{"name": "id"}, {"name": "data"}],
    target_lag="1 HOUR",
    warehouse="some_warehouse",
    refresh_mode="AUTO",
    initialize="ON_CREATE",
    as_="SELECT id, data FROM source_table",
    comment="This is a sample dynamic table",
    owner="SYSADMIN"
)
```


### YAML

```yaml
dynamic_table:
  name: some_dynamic_table
  columns:
    - name: id
    - name: data
  target_lag: "1 HOUR"
  warehouse: some_warehouse
  refresh_mode: AUTO
  initialize: ON_CREATE
  as_: "SELECT id, data FROM source_table"
  comment: "This is a sample dynamic table"
  owner: SYSADMIN
```


## Fields

* `name` (string, required) - The name of the dynamic table.
* `columns` (list, required) - A list of dicts defining the structure of the table.
* `target_lag` (string) - The acceptable lag (delay) for data in the table. Defaults to "DOWNSTREAM".
* `warehouse` (string or [Warehouse](warehouse.md), required) - The warehouse where the table is stored.
* `as_` (string, required) - The query used to populate the table.
* `refresh_mode` (string or [RefreshMode](refresh_mode.md)) - The mode of refreshing the table (AUTO, FULL, INCREMENTAL).
* `initialize` (string or [InitializeBehavior](initialize_behavior.md)) - The behavior when the table is initialized (ON_CREATE, ON_SCHEDULE).
* `comment` (string) - An optional comment for the table.
* `owner` (string or [Role](role.md)) - The owner of the table. Defaults to "SYSADMIN".


