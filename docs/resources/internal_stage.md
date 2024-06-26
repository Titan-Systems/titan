---
description: >-
  
---

# InternalStage

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/create-stage.html)

Represents an internal stage in Snowflake, which is a named location used to store data files
that will be loaded into or unloaded from Snowflake tables.


## Examples

### Python

```python
internal_stage = InternalStage(
    name="some_internal_stage",
    owner="SYSADMIN",
    encryption={"type": "SNOWFLAKE_SSE"},
    directory={"enable": True},
    tags={"department": "finance"},
    comment="Data loading stage"
)
```


### YAML

```yaml
stages:
  - name: some_internal_stage
    type: internal
    owner: SYSADMIN
    encryption:
      type: SNOWFLAKE_SSE
    directory:
      enable: true
    tags:
      department: finance
    comment: Data loading stage
```


## Fields

* `name` (string, required) - The name of the internal stage.
* `owner` (string or [Role](role.md)) - The owner role of the internal stage. Defaults to "SYSADMIN".
* `encryption` (dict) - A dictionary specifying encryption settings.
* `directory` (dict) - A dictionary specifying directory usage settings.
* `tags` (dict) - A dictionary of tags associated with the internal stage.
* `comment` (string) - A comment for the internal stage.


