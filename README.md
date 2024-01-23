# `Titan Core`

Titan Core helps you provision, deploy, and secure resources in Snowflake. It replaces infrastructure tools like Terraform or Schemachange.

Define any Snowflake resource, including users, roles, schemas, databases, integrations, pipes, stages, functions, and stored procedures, using declarative Python.

## How it works

### Define resources

```Python
from titan.resources import Grant, Role, Warehouse

role = Role(name="transformer")

warehouse = Warehouse(
    name="transforming",
    warehouse_size="large",
    auto_suspend=60,
)

grants = [
    Grant(priv="usage", to=role, on=warehouse),
    Grant(priv="operate", to=role, on=warehouse),
]
```

### Plan and apply
```Python
from titan import Blueprint

bp = Blueprint()
bp.add(
    role,
    warehouse,
    *grants,
)
plan = bp.plan(session)
print(plan) # =>
"""
account:ABC123

  » role.transformer will be created

  + role "urn::ABC123:role/transformer" {
     + name  = "transformer"
     + owner = "SYSADMIN"
    }

  + warehouse "urn::ABC123:warehouse/transforming" {
     + name           = "transforming"
     + owner          = "SYSADMIN"
     + warehouse_type = "STANDARD"
     + warehouse_size = "LARGE"
     + auto_suspend   = 60
    }

  + grant "urn::ABC123:grant/..." {
     + priv = "USAGE"
     + on   = warehouse "transforming"
     + to   = role "transformer
    }

  + grant "urn::ABC123:grant/..." {
     + priv = "OPERATE"
     + on   = warehouse "transforming"
     + to   = role "transformer
    }
"""
bp.apply(session, plan)
```

## Titan vs other tools

| Feature/Capability                      | Titan Core     | Terraform      | Schemachange   |
|-----------------------------------------|----------------|----------------|----------------|
| Plan and Execute Changes                | ✅              | ✅              | ❌              |
| Declarative Configuration               | ✅              | ✅              | ❌              |
| Python-Based Definitions                | ✅              | ❌              | ❌              |
| SQL Support                             | ✅              | ❌              | ✅              |
| Multi-Role Support                      | ✅              | ❌              | N/A            |
| No State File Dependency                | ✅              | ❌              | ✅              |
| Checks for Required Privileges          | ✅              | ❌              | ❌              |
| Infrastructure Visualization            | WIP             | ✅              | ❌              |


### Titan Core vs Terraform
Terraform is an infrastructure-as-code tool using the HCL config language.

The Snowflake provider for Terraform is limited to **1 role per provider**. This limitation is at odds with Snowflake's design, which is built to use multiple roles. This mismatch forces you into a complex multi-provider setup which can result in drift, permission errors, and broken plans.

Titan Core streamlines this with upfront privileges checks to ensure that plans can be applied. When privileges are missing, Titan tells you exactly what to grant. This speeds up development cycles and helps eliminate the use of `ACCOUNTADMIN`.

Titan also doesn’t use a state file, which provides more accurate plans and eliminates state mismatch issues.


### Titan Core vs Schemachange
Schemachange is a database migration tool that uses SQL scripts to deploy resources to different environments. As an imperative migration tool, it requires developers to write code for each step, demanding a deep understanding of the database's current state and the exact commands needed for updates. If environments change, your Schemachange scripts may need significant adjustments.

Titan Core simplifies this process with a declarative Python approach. It allows you to define what your environment should look like, without specifying the detailed steps to get there. This is less error-prone and more flexible to changes. Titan Core manages a broader range of Snowflake resources, providing a more integrated and streamlined experience, especially in dynamic and complex data environments.


## Installation

```SQL
-- AWS
CREATE DATABASE titan;

CREATE STAGE titan.public.titan_aws
  URL = 's3://titan-snowflake/';

EXECUTE IMMEDIATE
  FROM @titan.public.titan_aws/install;
```

Install Titan from GitHub with pip:

```bash
python -m pip install git+https://github.com/teej/titan.git
```

## Examples

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
    bp = Blueprint(
        name="dbt-quickstart",
        account=os.environ["SNOWFLAKE_ACCOUNT"],
    )
    bp.add(*dbt())
    session = snowflake.connector.connect(**connection_params)
    plan = bp.plan(session)
    
    # Update Snowflake to match blueprint
    bp.apply(session, plan)

```
