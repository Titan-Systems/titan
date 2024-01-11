# Titan


## What is Titan?

Titan is a Python library to manage data warehouse infrastructure.

Titan is made up of many parts:

- **Titan Resource API**. Manage resources with pure-Python classes.
  
- **Titan Blueprint**. Define infrastructure with code.


## Why Use Titan?

Titan provides a simple way to manage your data warehouse. With Titan, you can:

1. **Declarative API**: Describe what you want without the hassle of how to achieve it.

2. **Deferred Execution**: Plan your infrastructure modifications without immediate execution, allowing you to visualize and review changes before they happen.

3. **SQL Compatibility**: Integrate your existing SQL scripts and workflows into Titan, ensuring a smooth transition and continuous functionality.

4. **Type Checking**: Titan ensures that the resources and configurations you define are correctly typed, reducing the chances of runtime errors.

## Installation

Install Titan from GitHub with pip:

```bash
python -m pip install git+https://github.com/teej/titan.git
```

## Getting Started

Use Titan to create a starter dbt project.

```Python

import os
import snowflake.connector

from titan import Blueprint
from titan.resources import Database, Warehouse, Role, User, RoleGrant

connection_params = {
    "account": os.environ["SNOWFLAKE_ACCOUNT"],
    "user": os.environ["SNOWFLAKE_USER"],
    "password": os.environ["SNOWFLAKE_PASSWORD"],
}


def dbt():
    # Databases
    raw_db = Database(name="RAW")
    analytics_db = Database(name="ANALYTICS")

    # Warehouses
    loading_wh = Warehouse(name="LOADING")
    transforming_wh = Warehouse(name="TRANSFORMING")
    reporting_wh = Warehouse(
        name="REPORTING",
        warehouse_size="SMALL",
        auto_suspend=60,
    )

    # Roles
    loader = Role(name="LOADER")
    transformer = Role(name="TRANSFORMER")
    reporter = Role(name="REPORTER")

    # Users
    user = User(name="TEEJ", must_change_password=False, default_role=reporter.name)

    # GRANTS
    user_grant = RoleGrant(role=reporter, to_user=user)
    sysadmin_grants = [
        RoleGrant(role=loader, to_role="SYSADMIN"),
        RoleGrant(role=transformer, to_role="SYSADMIN"),
        RoleGrant(role=reporter, to_role="SYSADMIN"),
    ]

    return (
        raw_db,
        analytics_db,
        loading_wh,
        transforming_wh,
        reporting_wh,
        loader,
        transformer,
        reporter,
        user,
        user_grant,
        *sysadmin_grants,
    )


if __name__ == "__main__":
    bp = Blueprint(name="dbt-quickstart", account=os.environ["SNOWFLAKE_ACCOUNT"])
    bp.add(*dbt())
    session = snowflake.connector.connect(**connection_params)
    plan = bp.plan(session)
    
    # Update Snowflake to match blueprint
    bp.apply(session, plan)

```
