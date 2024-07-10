---
description: >-
  
---

# PythonStoredProcedure

[Snowflake Documentation](s://docs.snowflake.com/en/sql-reference/sql/create-procedure)

Represents a Python stored procedure in Snowflake, allowing for the execution of Python code within the Snowflake environment.


## Examples

### Python

```python
procedure = PythonStoredProcedure(
    name="some_procedure",
    args=[],
    returns="STRING",
    runtime_version="3.8",
    packages=["snowflake-snowpark-python"],
    handler="process_data",
    as_="def process_data(): return 'Hello, World!'",
    comment="A simple procedure",
    copy_grants=False,
    execute_as="CALLER",
    external_access_integrations=None,
    imports=None,
    null_handling="CALLED_ON_NULL_INPUT",
    owner="SYSADMIN",
    secure=False
)
```


### YAML

```yaml
procedures:
- name: some_procedure
    args: []
    returns: STRING
    runtime_version: "3.8"
    packages:
    - snowflake-snowpark-python
    handler: process_data
    as_: "def process_data(): return 'Hello, World!'"
    comment: "A simple procedure"
    copy_grants: false
    execute_as: CALLER
    external_access_integrations: null
    imports: null
    null_handling: CALLED_ON_NULL_INPUT
    owner: SYSADMIN
    secure: false
```


## Fields

* `name` (str, required) - The name of the procedure.
* `args` (list) - The arguments of the procedure.
* `returns` ([DataType](data_type.md)) - The data type of the return value.
* `runtime_version` (str, required) - The Python runtime version.
* `packages` (list) - The list of packages required by the procedure.
* `handler` (str, required) - The handler function for the procedure.
* `as_` (str) - The procedure definition.
* `comment` (str) - A comment about the procedure. Defaults to "user-defined procedure".
* `copy_grants` (bool) - Whether to copy grants. Defaults to False.
* `execute_as` ([ExecutionRights](execution_rights.md)) - The execution rights. Defaults to ExecutionRights.CALLER.
* `external_access_integrations` (list) - External access integrations if any.
* `imports` (list) - Files to import.
* `null_handling` ([NullHandling](null_handling.md)) - How nulls are handled. Defaults to NullHandling.CALLED_ON_NULL_INPUT.
* `owner` (string or [Role](role.md)) - The owner of the procedure. Defaults to "SYSADMIN".
* `secure` (bool) - Whether the procedure is secure. Defaults to False.


