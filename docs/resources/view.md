---
description: >-
  
---

# View

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/create-view)

Represents a view in Snowflake, which is a virtual table created by a stored query on the data.
Views are used to simplify complex queries, improve security, or enhance performance.


## Examples

### Python

```python
view = View(
    name="some_view",
    owner="SYSADMIN",
    secure=True,
    as_="SELECT * FROM some_table"
)
```


### YAML

```yaml
views:
  - name: some_view
    owner: SYSADMIN
    secure: true
    as_: "SELECT * FROM some_table"
```


## Fields

* `name` (string, required) - The name of the view.
* `owner` (string or [Role](role.md)) - The owner role of the view. Defaults to "SYSADMIN".
* `secure` (bool) - Specifies if the view is secure.
* `volatile` (bool) - Specifies if the view is volatile.
* `recursive` (bool) - Specifies if the view is recursive.
* `columns` (list) - A list of dictionaries specifying column details.
* `tags` (dict) - A dictionary of tags associated with the view.
* `change_tracking` (bool) - Specifies if change tracking is enabled.
* `copy_grants` (bool) - Specifies if grants should be copied from the base table.
* `comment` (string) - A comment for the view.
* `as_` (string) - The SELECT statement defining the view.


