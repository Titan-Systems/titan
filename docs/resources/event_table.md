---
description: >-
  
---

# EventTable

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/create-event-table)

An event table captures events, including logged messages from functions and procedures.


## Examples

### Python

```python
event_table = EventTable(
    name="some_event_table",
    cluster_by=["timestamp", "user_id"],
    data_retention_time_in_days=365,
    max_data_extension_time_in_days=30,
    change_tracking=True,
    default_ddl_collation="utf8",
    copy_grants=True,
    comment="This is a sample event table.",
    tags={"department": "analytics"}
)
```


### YAML

```yaml
event_tables:
  - name: some_event_table
    cluster_by:
      - timestamp
      - user_id
    data_retention_time_in_days: 365
    max_data_extension_time_in_days: 30
    change_tracking: true
    default_ddl_collation: utf8
    copy_grants: true
    comment: This is a sample event table.
    tags:
      department: analytics
```


## Fields

* `name` (string, required) - The name of the event table.
* `cluster_by` (list) - The expressions to cluster data by.
* `data_retention_time_in_days` (int) - The number of days to retain data.
* `max_data_extension_time_in_days` (int) - The maximum number of days to extend data retention.
* `change_tracking` (bool) - Specifies whether change tracking is enabled. Defaults to False.
* `default_ddl_collation` (string) - The default collation for DDL operations.
* `copy_grants` (bool) - Specifies whether to copy grants. Defaults to False.
* `comment` (string) - A comment for the event table.
* `tags` (dict) - Tags associated with the event table.


