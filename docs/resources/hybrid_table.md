---
description: >-
  
---

# HybridTable

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/create-hybrid-table)

`[UNDER DEVELOPMENT]`
A hybrid table is a Snowflake table type that is optimized for hybrid transactional and operational workloads that require low latency and high throughput on small random point reads and writes.


## Examples

### Python

```python
hybrid_table = HybridTable(
    name="some_hybrid_table",
    columns=[Column(name="col1", type="STRING")],
    owner="SYSADMIN",
    comment="This is a hybrid table."
)
```


### YAML

```yaml
hybrid_tables:
  - name: some_hybrid_table
    columns:
      - name: col1
        type: STRING
    owner: SYSADMIN
    comment: This is a hybrid table.
```


## Fields

* `name` (string, required) - The name of the hybrid table.
* `columns` (list, required) - The columns of the hybrid table.
* `tags` (dict) - Tags associated with the hybrid table.
* `owner` (string or [Role](role.md)) - The owner role of the hybrid table. Defaults to "SYSADMIN".
* `comment` (string) - A comment for the hybrid table.


