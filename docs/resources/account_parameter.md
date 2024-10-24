---
description: >-
  
---

# AccountParameter

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/alter-account)

An account parameter in Snowflake that allows you to set or alter account-level parameters.


## Examples

### Python

```python
account_parameter = AccountParameter(
    name="some_parameter",
    value="some_value",
)
```


### YAML

```yaml
account_parameters:
  - name: some_parameter
    value: some_value
```


## Fields

* `name` (string, required) - The name of the account parameter.
* `value` ([Any](any.md), required) - The value to set for the account parameter.


