---
description: >-
  
---

# Pipe

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/create-pipe)

Represents a data ingestion pipeline in Snowflake, which automates the loading of data into tables.


## Examples

### Python

```python
pipe = Pipe(
    name="some_pipe",
    as_="COPY INTO some_table FROM @%some_stage",
    owner="SYSADMIN",
    auto_ingest=True,
    error_integration="some_integration",
    aws_sns_topic="some_topic",
    integration="some_integration",
    comment="This is a sample pipe"
)
```


### YAML

```yaml
pipes:
  - name: some_pipe
    as_: "COPY INTO some_table FROM @%some_stage"
    owner: SYSADMIN
    auto_ingest: true
    error_integration: some_integration
    aws_sns_topic: some_topic
    integration: some_integration
    comment: "This is a sample pipe"
```


## Fields

* `name` (string, required) - The name of the pipe.
* `as_` (string, required) - The SQL statement that defines the data loading operation.
* `owner` (string or [Role](role.md)) - The owner role of the pipe. Defaults to "SYSADMIN".
* `auto_ingest` (bool) - Specifies if the pipe automatically ingests data when files are added to the stage. Defaults to None.
* `error_integration` (string) - The name of the integration used for error notifications. Defaults to None.
* `aws_sns_topic` (string) - The AWS SNS topic where notifications are sent. Defaults to None.
* `integration` (string) - The integration used for data loading. Defaults to None.
* `comment` (string) - A comment for the pipe. Defaults to None.


