---
description: >-
  
---

# Share

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/create-share)

Represents a share resource in Snowflake, which allows sharing data across Snowflake accounts.


## Examples

### Python

```python
share = Share(
    name="some_share",
    comment="This is a snowflake share."
)
```


### YAML

```yaml
shares:
  - name: some_share
    comment: This is a snowflake share.
```


## Fields

* `name` (string, required) - The name of the share.
* `owner` (string or [Role](role.md)) - The owner of the share. Defaults to "ACCOUNTADMIN".
* `comment` (string) - A comment about the share.


