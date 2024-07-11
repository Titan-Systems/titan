---
description: >-
  
---

# MaterializedView

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/create-materialized-view)

A Materialized View in Snowflake is a database object that contains the results of a query.
It is physically stored and automatically updated as data changes, providing faster access to data.


## Examples

### Python

```python
materialized_view = MaterializedView(
    name="some_materialized_view",
    owner="SYSADMIN",
    secure=True,
    as_="SELECT * FROM some_table",
)
```


### YAML

```yaml
materialized_views:
  - name: some_materialized_view
    owner: SYSADMIN
    secure: true
    as_: SELECT * FROM some_table
```


## Fields

* `name` (string, required) - The name of the materialized view.
* `owner` (string or [Role](role.md)) - The owner role of the materialized view. Defaults to "SYSADMIN".
* `secure` (bool) - Specifies if the materialized view is secure. Defaults to False.
* `columns` (list) - A list of dictionaries specifying column definitions.
* `tags` (dict) - Tags associated with the materialized view.
* `copy_grants` (bool) - Specifies if grants should be copied from the source. Defaults to False.
* `comment` (string) - A comment for the materialized view.
* `cluster_by` (list) - A list of expressions defining the clustering of the materialized view.
* `as_` (string, required) - The SELECT statement used to populate the materialized view.


