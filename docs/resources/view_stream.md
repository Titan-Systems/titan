---
description: >-
  
---

# ViewStream

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/create-stream)

Represents a stream on a view in Snowflake, allowing for real-time data processing and querying.
This stream can be configured with various options such as time travel, append-only mode, and initial row visibility.


## Examples

### Python

```python
view_stream = ViewStream(
    name="some_stream",
    on_view="some_view",
    owner="SYSADMIN",
    copy_grants=True,
    at={"TIMESTAMP": "2022-01-01 00:00:00"},
    before={"STREAM": "some_other_stream"},
    append_only=False,
    show_initial_rows=True,
    comment="This is a sample stream on a view."
)
```


### YAML

```yaml
streams:
  - name: some_stream
    on_view: some_view
    owner: SYSADMIN
    copy_grants: true
    at:
      TIMESTAMP: "2022-01-01 00:00:00"
    before:
      STREAM: some_other_stream
    append_only: false
    show_initial_rows: true
    comment: This is a sample stream on a view.
```


## Fields

* `name` (string, required) - The name of the stream.
* `on_view` (string, required) - The name of the view the stream is based on.
* `owner` (string or [Role](role.md)) - The role that owns the stream. Defaults to 'SYSADMIN'.
* `copy_grants` (bool) - Whether to copy grants from the view to the stream.
* `at` (dict) - A dictionary specifying the point in time for the stream to start, using keys like TIMESTAMP, OFFSET, STATEMENT, or STREAM.
* `before` (dict) - A dictionary specifying the point in time for the stream to start, similar to 'at' but defining a point before the specified time.
* `append_only` (bool) - If set to True, the stream records only append operations.
* `show_initial_rows` (bool) - If set to True, the stream includes the initial rows of the view at the time of stream creation.
* `comment` (string) - An optional description for the stream.


