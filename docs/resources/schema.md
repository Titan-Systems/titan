---
description: >-
  
---

# Schema

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/create-schema)

Represents a schema in Snowflake, which is a logical grouping of database objects such as tables, views, and stored procedures. Schemas are used to organize and manage such objects within a database.


## Examples

### Python

```python
schema = Schema(
    name="some_schema",
    transient=True,
    managed_access=True,
    data_retention_time_in_days=7,
    max_data_extension_time_in_days=28,
    default_ddl_collation="utf8",
    tags={"project": "analytics"},
    owner="SYSADMIN",
    comment="Schema for analytics project."
)
```


### YAML

```yaml
schemas:
  - name: some_schema
    transient: true
    managed_access: true
    data_retention_time_in_days: 7
    max_data_extension_time_in_days: 28
    default_ddl_collation: utf8
    tags:
      project: analytics
    owner: SYSADMIN
    comment: Schema for analytics project.
```


## Fields

* `name` (string, required) - The name of the schema.
* `transient` (bool) - Specifies if the schema is transient. Defaults to False.
* `managed_access` (bool) - Specifies if the schema has managed access. Defaults to False.
* `data_retention_time_in_days` (int) - The number of days to retain data. Defaults to 1.
* `max_data_extension_time_in_days` (int) - The maximum number of days to extend data retention. Defaults to 14.
* `default_ddl_collation` (string) - The default DDL collation setting.
* `tags` (dict) - Tags associated with the schema.
* `owner` (string or [Role](role.md)) - The owner of the schema. Defaults to "SYSADMIN".
* `comment` (string) - A comment about the schema.


