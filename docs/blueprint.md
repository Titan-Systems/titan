# Blueprint

The Blueprint class validates and deploys a set of resources as a group. It provides a structured way to manage your Snowflake resources through two methods: `plan` and `apply`.

Blueprint provides options to customize how resources are deployed to Snowflake, including `run_mode`, `allowlist`, and `dry_run`.

In Python, you utilize the `Blueprint` class to create and manage blueprints. When using the CLI or GitHub Action with YAML configurations, a Blueprint is created automatically.

## Example

```Python
from titan.blueprint import Blueprint
from titan.resources import Database, Schema

bp = Blueprint(
    run_mode='create-or-update',
    resources=[
        Database('my_database'),
        Schema('my_schema', database='my_database'),
    ]
)
plan = bp.plan(session)
bp.apply(session, plan)
```

## Blueprint parameters
- **run_mode** (`str`): Defines how the blueprint interacts with the Snowflake account
  - **create-or-update** (*default*): Resources are either created or updated, no resources are destroyed
  - **sync**: Modifies your Snowflake account to match the blueprint exactly. When in use, `allowlist` must be specified. ⚠️`WARNING`⚠️: Resources not defined in the blueprint but present in your account will be dropped.
- **resources** (`list[Resource]`): List of resources initialized in the blueprint.
- **allowlist** (`list[str]`): Specifies the allowed resource types in the blueprint.
- **dry_run** (`bool`): `apply()` will return a list of SQL commands that would be executed without applying them.

## Methods

### `plan(session)`

The plan method analyzes your Snowflake account to determine how it is different from your configuration. It identifies what resources need to be added, changed, or removed to achieve the desired state.

#### Parameters:
- **session** (`SnowflakeConnection`): The session object used to connect to Snowflake

#### Returns:

- `list[ResourceChange]`: The list of changes that need to be made to the Snowflake account


### `apply(session, [plan])`

The apply method executes the SQL commands required to update your Snowflake account according to the plan generated. Apply returns a list of SQL commands that were executed.

#### Parameters:
- **session** (`SnowflakeConnection`): The session object used to connect to Snowflake
- **plan** (`list[ResourceChange]`, *optional*): The list of changes to apply. If not provided, the plan is generated automatically.

#### Returns:

- `list[str]`: A list of SQL commands that were executed.


### `add(resource: Resource)`

Alternate uses:
- `add(resource_1, resource_2, ...)`
- `add([resource_1, resource_2, ...])`

The add method allows you to add a resource to the blueprint.
