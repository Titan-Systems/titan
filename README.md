# `Titan Core` - Snowflake infrastructure as code

<div align="center">
    <img src="./images/github-explainer.png" style="padding-bottom: 20px; width: 830px;"/>
</div>

Titan Core helps you provision, deploy, and secure resources in Snowflake. It replaces infrastructure tools like Terraform or Schemachange.

Define any Snowflake resource, including users, roles, schemas, databases, integrations, pipes, stages, functions, and stored procedures, using declarative Python.



# Installation

The easiest way to get started with Titan is to use the Titan Core Snowflake app.

## Snowflake app installation

```SQL
-- AWS
USE ROLE SYSADMIN;

CREATE DATABASE titan;

CREATE STAGE titan.public.titan_aws
  URL = 's3://titan-snowflake/';

EXECUTE IMMEDIATE
  FROM @titan.public.titan_aws/install;
```

The Titan Core Snowflake app includes a suite of stored procedures for managing Snowflake resources

```SQL
// Create or Update
create_or_update_database(config OBJECT, dry_run BOOLEAN): OBJECT
create_or_update_schema(config OBJECT, dry_run BOOLEAN): OBJECT
create_or_update_user(config OBJECT, dry_run BOOLEAN): OBJECT
create_or_update_warehouse(config OBJECT, dry_run BOOLEAN): OBJECT
create_or_update_role(config OBJECT, dry_run BOOLEAN): OBJECT

// Fetch
fetch_database(name VARCHAR): OBJECT
fetch_schema(name VARCHAR): OBJECT
fetch_user(name VARCHAR): OBJECT
fetch_warehouse(name VARCHAR): OBJECT
fetch_role(name VARCHAR): OBJECT
```

### Usage

```SQL
CALL fetch_schema(name => 'TITAN.PUBLIC');
// {
//   "comment": null,
//   "data_retention_time_in_days": 1,
//   "default_ddl_collation": null,
//   "managed_access": false,
//   "max_data_extension_time_in_days": 14,
//   "name": "PUBLIC",
//   "owner": "SYSADMIN",
//   "transient": false
// }
```

## Python API installation

If you want to run custom Titan code or use Titan on the command line, you can use the Titan Core Python API.

```bash
python -m pip install git+https://github.com/teej/titan.git
```

### Usage

Define resource configurations by instantiating Python objects.

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

Use Blueprint to apply those changes to your Snowflake account. Blueprint works similar to Terraform - add resources, call `plan(...)` to see what changes will be applied, and then call `apply(...)` to run the changes.

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

# Titan vs others

| Feature/Capability                      | Titan Core     | Terraform      | Schemachange   |
|-----------------------------------------|----------------|----------------|----------------|
| Plan and Execute Changes                | ✅             | ✅              | ❌             |
| Declarative Configuration               | ✅             | ✅              | ❌             |
| Python-Based Definitions                | ✅             | w/ CDKTF        | ❌             |
| SQL Support                             | ✅             | ❌              | ✅             |
| Multi-Role Support                      | ✅             | ❌              | N/A            |
| No State File Dependency                | ✅             | ❌              | ✅             |
| Checks for Required Privileges          | ✅             | ❌              | ❌             |
| Infrastructure Visualization            | WIP            | ✅              | ❌             |


## Titan Core vs Terraform
Terraform is an infrastructure-as-code tool using the HCL config language.

The Snowflake provider for Terraform is limited to **1 role per provider**. This limitation is at odds with Snowflake's design, which is built to use multiple roles. This mismatch forces you into a complex multi-provider setup which can result in drift, permission errors, and broken plans.

Titan Core streamlines this with upfront privileges checks to ensure that plans can be applied. When privileges are missing, Titan tells you exactly what to grant. This speeds up development cycles and helps eliminate the use of `ACCOUNTADMIN`.

Titan also doesn’t use a state file, which provides more accurate plans and eliminates state mismatch issues.


## Titan Core vs Schemachange
Schemachange is a database migration tool that uses SQL scripts to deploy resources to different environments. As an imperative migration tool, it requires developers to write code for each step, demanding a deep understanding of the database's current state and the exact commands needed for updates. If environments change, your Schemachange scripts may need significant adjustments.

Titan Core simplifies this process with a declarative Python approach. It allows you to define what your environment should look like, without specifying the detailed steps to get there. This is less error-prone and more flexible to changes. Titan Core manages a broader range of Snowflake resources, providing a more integrated and streamlined experience, especially in dynamic and complex data environments.



# Resource support

| Name                          | Supported | SPI |
|-------------------------------|-----------|-----|
| **Account Resources**         |           |     |
| API Integration               | ✅         | ❌  |
| Catalog Integration           | ❌         | ❌  |
| Compute Pool                  | ❌         | ❌  |
| Connection                    | ❌         | ❌  |
| Database                      | ✅         | ✅  |
| External Access Integration   | ✅         | ❌  |
| External Volume               | ❌         | ❌  |
| Grant                         | ✅         | ❌  |
| ↳ Future Grant                | WIP        | ❌  |
| ↳ Privilege Grant             | ✅         | ❌  |
| ↳ Role Grant                  | ✅         | ❌  |
| Network Policy                | ✅         | ❌  |
| Notification Integration      | WIP        | ❌  |
| ↳ Email                       | ✅         | ❌  |
| ↳ AWS                         | ❌         | ❌  |
| ↳ Azure                       | ❌         | ❌  |
| ↳ GCP                         | ❌         | ❌  |
| Replication Group             | ✅         | ❌  |
| Resource Monitor              | ✅         | ❌  |
| Role                          | ✅         | ✅  |
| Security Integration          | ❌         | ❌  |
| Share                         | ❌         | ❌  |
| Storage Integration           | WIP        | ❌  |
| ↳ AWS                         | ✅         | ❌  |
| ↳ Azure                       | ✅         | ❌  |
| ↳ GCP                         | ✅         | ❌  |
| User                          | ✅         | ✅  |
| Warehouse                     | ✅         | ✅  |
|                               |            |    |
| **Database Resources**        |            |    |
| Database Role                 | ✅         | ❌  |
| Schema                        | ✅         | ✅  |
|                               |            |    |
| **Schema Resources**          |            |    |
| Alert                         | ✅         | ❌  |
| Aggregation Policy            | ❌         | ❌  |
| Dynamic Table                 | ✅         | ❌  |
| Event Table                   | ✅         | ❌  |
| External Function             | ✅         | ❌  |
| External Stage                | ✅         | ❌  |
| External Table                | ❌         | ❌  |
| Failover Group                | ✅         | ❌  |
| File Format                   | ❌         | ❌  |
| ↳ CSV                         | ❌         | ❌  |
| ↳ JSON                        | ❌         | ❌  |
| ↳ AVRO                        | ❌         | ❌  |
| ↳ ORC                         | ❌         | ❌  |
| ↳ Parquet                     | ❌         | ❌  |
| Iceberg Table                 | ❌         | ❌  |
| Image Repository              | ❌         | ❌  |
| Internal Stage                | ✅         | ❌  |
| Masking Policy                | ❌         | ❌  |
| Materialized View             | ❌         | ❌  |
| Model                         | ❌         | ❌  |
| Network Rule                  | ✅         | ❌  |
| Packages Policy               | ✅         | ❌  |
| Password Policy               | ✅         | ❌  |
| Pipe                          | ✅         | ❌  |
| Projection Policy             | ❌         | ❌  |
| Role Grant                    | ✅         | ❌  |
| Row Access Policy             | ❌         | ❌  |
| Secret                        | ✅         | ❌  |
| Sequence                      | ✅         | ❌  |
| Service                       | ❌         | ❌  |
| Session Policy                | ❌         | ❌  |
| Stage                         | ✅         | ❌  |
| ↳ External                    | ✅         | ❌  |
| ↳ Internal                    | ✅         | ❌  |
| Stored Procedure              | WIP        | ❌  |
| ↳ Java                        | ❌         | ❌  |
| ↳ Javascript                  | ❌         | ❌  |
| ↳ Python                      | ✅         | ❌  |
| ↳ Scala                       | ❌         | ❌  |
| ↳ SQL                         | ❌         | ❌  |
| Stream                        | WIP        | ❌  |
| ↳ External Table              | ❌         | ❌  |
| ↳ Stage                       | ✅         | ❌  |
| ↳ Table                       | ✅         | ❌  |
| ↳ View                        | ✅         | ❌  |
| Streamlit                     | ❌         | ❌  |
| Table                         | ✅         | ❌  |
| Tag                           | ✅         | ❌  |
| Task                          | ✅         | ❌  |
| User-Defined Function         | WIP        | ❌  |
| ↳ Java                        | ❌         | ❌  |
| ↳ Javascript                  | ✅         | ❌  |
| ↳ Python                      | ✅         | ❌  |
| ↳ Scala                       | ❌         | ❌  |
| ↳ SQL                         | ❌         | ❌  |
| View                          | ✅         | ❌  |



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
    bp = Blueprint(name="dbt-quickstart")
    bp.add(*dbt())
    session = snowflake.connector.connect(**connection_params)
    plan = bp.plan(session)
    
    # Update Snowflake to match blueprint
    bp.apply(session, plan)

```
