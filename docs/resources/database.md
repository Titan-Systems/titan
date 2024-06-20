---
description: >-
  
---

# Database

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/create-database)

Represents a database in Snowflake.


## Examples

### Python

```python
database = Database(
    name="some_database",
    transient=True,
    owner="SYSADMIN",
    data_retention_time_in_days=7,
    max_data_extension_time_in_days=28,
    default_ddl_collation="utf8",
    tags={"project": "research", "priority": "high"},
    comment="This is a database."
)
```
A database can contain schemas. In Python, you can add a schema to a database in several ways:
By database name:
```python
sch = Schema(
    name = "some_schema",
    database = "my_test_db",
)
```
By database object:
```python
db = Database(name = "my_test_db")
sch = Schema(
    name = "some_schema",
    database = db,
)
```
Or using the `add` method:
```python
db = Database(name = "my_test_db")
sch = Schema(name = "some_schema")
db.add(sch)
```


### YAML

```yaml
databases:
  - name: some_database
    transient: true
    owner: SYSADMIN
    data_retention_time_in_days: 7
    max_data_extension_time_in_days: 28
    default_ddl_collation: utf8
    tags:
      project: research
      priority: high
    comment: This is a database.
```
In yaml, you can add schemas to a database using the `schemas` field:
```yaml
databases:
  - name: some_database
    schemas:
      - name: another_schema
```
Or by name:
```yaml
databases:
  - name: some_database
schemas:
    - name: another_schema
      database: some_database
```


## Fields

* `name` (string, required) - The name of the database.
* `transient` (bool) - Specifies if the database is transient. Defaults to False.
* `owner` (string or [Role](role.md)) - The owner role of the database. Defaults to "SYSADMIN".
* `data_retention_time_in_days` (int) - The number of days to retain data. Defaults to 1.
* `max_data_extension_time_in_days` (int) - The maximum number of days to extend data retention. Defaults to 14.
* `default_ddl_collation` (string) - The default collation for DDL statements.
* `tags` (dict) - A dictionary of tags associated with the database.
* `comment` (string) - A comment describing the database.


