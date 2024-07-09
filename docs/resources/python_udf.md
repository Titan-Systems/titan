---
description: >-
  
---

# PythonUDF

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/create-function)

A Python user-defined function (UDF) in Snowflake allows users to define their own custom functions in Python.
These functions can be used to perform operations that are not available as standard SQL functions.


## Examples

### Python

```python
python_udf = PythonUDF(
    name="some_python_udf",
    returns="string",
    runtime_version="3.8",
    handler="process_data",
    args=[{"name": "input_data", "data_type": "string"}],
    as_="process_data_function",
    comment="This function processes data.",
    copy_grants=False,
    external_access_integrations=["s3_integration"],
    imports=["pandas", "numpy"],
    null_handling="CALLED_ON_NULL_INPUT",
    owner="SYSADMIN",
    packages=["pandas", "numpy"],
    secrets={"api_key": "secret_value"},
    secure=False,
    volatility="IMMUTABLE"
)
```


### YAML

```yaml
python_udfs:
  - name: some_python_udf
    returns: string
    runtime_version: 3.8
    handler: process_data
    args:
      - name: input_data
        data_type: string
    as_: process_data_function
    comment: This function processes data.
    copy_grants: false
    external_access_integrations:
      - s3_integration
    imports:
      - pandas
      - numpy
    null_handling: CALLED_ON_NULL_INPUT
    owner: SYSADMIN
    packages:
      - pandas
      - numpy
    secrets:
      api_key: secret_value
    secure: false
    volatility: IMMUTABLE
```


## Fields

* `name` (string, required) - The name of the function.
* `returns` (string, required) - The data type of the function's return value.
* `runtime_version` (string, required) - The version of the Python runtime to use.
* `handler` (string, required) - The name of the method to call in the Python script.
* `args` (list, required) - A list of arguments that the function takes.
* `as_` (string) - The Python code to execute when the function is called.
* `comment` (string) - A comment for the function.
* `copy_grants` (bool) - Whether to copy grants from the existing function. Defaults to False.
* `external_access_integrations` (list) - List of external integrations accessible by the function.
* `imports` (list) - List of modules to import in the function.
* `null_handling` ([NullHandling](null_handling.md)) - Specifies how NULL values are handled by the function.
* `owner` (string or [Role](role.md)) - The owner role of the function. Defaults to "SYSADMIN".
* `packages` (list) - List of Python packages that the function can use.
* `secrets` (dict) - Secrets that can be accessed by the function.
* `secure` (bool) - Whether the function is secure. Defaults to False.
* `volatility` (string or [Volatility](volatility.md)) - The volatility of the function.


