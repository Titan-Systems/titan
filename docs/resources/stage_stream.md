---
description: >-
  
---

# StageStream

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/create-stream)

Represents a stream on a stage in Snowflake, which allows for capturing data changes on the stage.


## Examples

### Python

```python
stream = StageStream(
    name="some_stream",
    on_stage="some_stage",
    owner="SYSADMIN",
    copy_grants=True,
    comment="This is a sample stream."
)
```


### YAML

```yaml
streams:
  - name: some_stream
    on_stage: some_stage
    owner: SYSADMIN
    copy_grants: true
    comment: This is a sample stream.
```


## Fields

* `name` (string, required) - The name of the stream.
* `on_stage` (string, required) - The name of the stage the stream is based on.
* `owner` (string or [Role](role.md)) - The role that owns the stream. Defaults to "SYSADMIN".
* `copy_grants` (bool) - Whether to copy grants from the source stage to the stream.
* `comment` (string) - An optional description for the stream.


