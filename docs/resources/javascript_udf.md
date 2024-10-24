---
description: >-
  
---

# JavascriptUDF

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/create-function)

A Javascript user-defined function (UDF) in Snowflake allows you to define a function using the JavaScript programming language.


## Examples

### Python

```python
js_udf = JavascriptUDF(
    name="some_function",
    returns="STRING",
    as_="function(x) { return x.toUpperCase(); }",
    args=[{"name": "x", "data_type": "STRING"}],
    comment="Converts a string to uppercase",
)
```


### YAML

```yaml
functions:
  - name: some_function
    returns: STRING
    as_: function(x) { return x.toUpperCase(); }
    args:
      - name: x
        data_type: STRING
    comment: Converts a string to uppercase
```


## Fields

* `name` (string, required) - The name of the function.
* `returns` (string or [DataType](data_type.md), required) - The data type of the function's return value.
* `as_` (string, required) - The JavaScript code to execute when the function is called.
* `args` (list) - The arguments that the function takes.
* `comment` (string) - A comment for the function.
* `copy_grants` (bool) - Specifies whether to retain the access privileges from the original function when a new function is created using CREATE OR REPLACE FUNCTION. Defaults to False.
* `external_access_integrations` (list) - External integrations accessible by the function.
* `handler` (string) - The entry point for the function within the JavaScript code.
* `imports` (list) - The list of JavaScript files to import.
* `null_handling` (string or [NullHandling](null_handling.md)) - How the function handles NULL input.
* `owner` (string or [Role](role.md)) - The owner of the function. Defaults to "SYSADMIN".
* `packages` (list) - The list of npm packages that the function depends on.
* `runtime_version` (string) - The JavaScript runtime version to use.
* `secrets` (dict of string to string) - Key-value pairs of secrets available to the function.
* `secure` (bool) - Specifies whether the function is secure. Defaults to False.
* `volatility` (string or [Volatility](volatility.md)) - The volatility of the function.


