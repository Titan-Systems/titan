# Blueprint

The Blueprint class validates and deploys a set of resources as a group. It provides a structured way to manage your Snowflake resources through two methods: `plan` and `apply`.

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

## Attributes
- **run_mode** (str): Defines how the blueprint interacts with the Snowflake account
  - **create-or-update**: Resources are either created or updated, no resources are destroyed
  - **sync**: Synchronizes your account to match the blueprint strictly. Resources not defined in the blueprint but present in your account will be dropped. This mode does not affect users, roles, grants, or tables for safety reasons.
  - **sync-all**: ⚠️`DANGEROUS`⚠️ mode that acts like `sync` but without restrictions, potentially leading to account lockout.
- **resources** (`list`): List of resources initialized in the blueprint.
- **allowlist** (`list`): Specifies the allowed resource types in the blueprint.
- **dry_run** (`bool`): Allows running the plan method without applying changes.


## Methods

### `plan(session)`

`plan` analyzes your Snowflake account to determine how it is different from your configuration. It identifies what resources need to be added, changed, or removed to achieve the desired state.

#### Parameters:
- **session** (`snowflake.connector.connect`): The session object used to connect to Snowflake

#### Returns:

- **list[ResourceChange]**: A list of `ResourceChange` objects that represent the changes that need to be made to the Snowflake account.


### `apply(session, [plan])`

The `apply` method executes the SQL commands required to update your Snowflake account according to the plan generated.

#### Parameters:
- **session**: A `snowflake.connector.connect` session object.
- **plan** (*optional*): A list of `ResourceChange` objects to apply. If not provided, the plan is generated automatically.

#### Returns:

- **list[str]**: A list of SQL commands that were executed.


### `add(resource: Resource)`

The `add` method allows you to add a resource to the blueprint. It supports multiple formats:

- `add(res1, res2, …)`
- `add([res1, res2, …])`




## Usage

In Python, you utilize the `Blueprint` class to create and manage blueprints. When using the CLI or GitHub Action with YAML configurations, a Blueprint is instantiated automatically, streamlining the process.

## Fields

- **run_mode**: Defines how the blueprint interacts with your Snowflake account.
  - **create-or-update**: Resources are either created or updated based on the blueprint; no resources are destroyed.
  - **sync**: Synchronizes your account to match the blueprint strictly. Resources not defined in the blueprint but present in your account will be dropped. This mode does not affect users, roles, grants, or tables for safety reasons.
  - **sync-all**: A hazardous mode that acts like `sync` but without restrictions, potentially leading to account lockout.

- **resources** (list): Initializes the blueprint with a list of resources.

- **allowlist** (list): Specifies the resource types allowed in the blueprint. If empty, all resource types are permitted.

- **dry_run**: Executes the plan method without applying the changes, allowing for a safe preview of the actions the blueprint would perform.

## Methods

- **Blueprint()**: Constructor for creating a blueprint instance.
- **.add(resource)**: Adds resources to the blueprint. Supports multiple formats:
  - `.add(res1, res2, …)`
  - `.add([res1, res2, …])`
- **.plan(connection)**: Generates a plan based on the current state of the Snowflake account and the blueprint.
- **.apply(connection, [plan])**: Applies the generated plan to the Snowflake account.

## Helpers

- **print_plan**: Utility function to print the plan in a readable format.

## Diagram

Below is an ASCII diagram illustrating the interactions between the blueprint methods and the resources:

