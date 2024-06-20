---
description: >-
  
---

# TableStream

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/create-stream)

Represents a stream on a table in Snowflake, which allows for change data capture on the table.


## Examples

### Python

```python
stream = TableStream(
    name="some_stream",
    on_table="some_table",
    owner="SYSADMIN",
    copy_grants=True,
    at={"TIMESTAMP": "2022-01-01 00:00:00"},
    before={"STREAM": "some_other_stream"},
    append_only=False,
    show_initial_rows=True,
    comment="This is a sample stream."
)
```


### YAML

```yaml
streams:
  - name: some_stream
    on_table: some_table
    owner: SYSADMIN
    copy_grants: true
    at:
      TIMESTAMP: "2022-01-01 00:00:00"
    before:
      STREAM: some_other_stream
    append_only: false
    show_initial_rows: true
    comment: This is a sample stream.
```


## Fields

* `name` (string, required) - The name of the stream.
* `on_table` (string, required) - The name of the table the stream is based on.
* `owner` (string or [Role](role.md)) - The role that owns the stream. Defaults to "SYSADMIN".
* `copy_grants` (bool) - Whether to copy grants from the source table to the stream.
* `at` (dict) - A dictionary specifying the point in time for the stream to start, using keys like TIMESTAMP, OFFSET, STATEMENT, or STREAM.
* `before` (dict) - A dictionary specifying the point in time for the stream to start, similar to 'at' but defining a point before the specified time.
* `append_only` (bool) - If set to True, the stream records only append operations.
* `show_initial_rows` (bool) - If set to True, the stream includes the initial rows of the table at the time of stream creation.
* `comment` (string) - An optional description for the stream.


