# Getting Started

If you're new, the best place to start is with the Python package.

### Python + CLI Installation

### Install from PyPi
coming soon

### Install from source

```sh
python -m venv .venv
source .venv/bin/activate
pip install git+https://github.com/Titan-Systems/titan.git
```

### Using the Python package

```Python
import os
import snowflake.connector

from titan.blueprint import Blueprint, print_plan
from titan.resources import Grant, Role, Warehouse

# Configure resources by instantiating Python objects.

role = Role(name="transformer")

warehouse = Warehouse(
    name="transforming",
    warehouse_size="large",
    auto_suspend=60,
)

usage_grant = Grant(priv="usage", to=role, on=warehouse)

# Titan compares your config to a Snowflake account. Create a Snowflake 
# connection to allow Titan to connect to your account.

connection_params = {
    "account": os.environ["SNOWFLAKE_ACCOUNT"],
    "user": os.environ["SNOWFLAKE_USER"],
    "password": os.environ["SNOWFLAKE_PASSWORD"],
    "role": "SYSADMIN",
}
session = snowflake.connector.connect(**connection_params)

# Create a Blueprint and pass your resources into it. A Blueprint helps you
# validate and deploy a set of resources.

bp = Blueprint(resources=[
    role,
    warehouse,
    usage_grant,
])

# Blueprint works like Terraform. Calling plan(...) will compare your config
# to the state of your Snowflake account and return a list of changes.

plan = bp.plan(session)
print_plan(plan) # =>
"""
» titan core
» Plan: 4 to add, 0 to change, 0 to destroy.

+ urn::ABCD123:warehouse/transforming {
  + name                                = "transforming"
  + owner                               = "SYSADMIN"
  + warehouse_type                      = "STANDARD"
  + warehouse_size                      = "LARGE"
  ...
}

+ urn::ABCD123:role/transformer {
  + name    = "transformer"
  + owner   = "USERADMIN"
  + tags    = None
  + comment = None
}

+ urn::ABCD123:grant/TRANSFORMER?priv=USAGE&on=warehouse/TRANSFORMING {
  + priv         = "USAGE"
  + on           = "transforming"
  + on_type      = "WAREHOUSE"
  + to           = TRANSFORMER
  ...
}
"""

# Calling apply(...) will convert your plan into the right set of SQL commands
# and run them against your Snowflake account.
bp.apply(session, plan) # =>
"""
[TITAN_USER:SYSADMIN]  > USE SECONDARY ROLES ALL
[TITAN_USER:SYSADMIN]  > CREATE WAREHOUSE TRANSFORMING warehouse_type = STANDARD ...
[TITAN_USER:SYSADMIN]  > USE ROLE USERADMIN
[TITAN_USER:USERADMIN] > CREATE ROLE TRANSFORMER
[TITAN_USER:USERADMIN] > USE ROLE SYSADMIN
[TITAN_USER:SYSADMIN]  > GRANT USAGE ON WAREHOUSE transforming TO TRANSFORMER
"""
```

### Using the CLI

You can use the CLI to generate a plan, apply a plan, or export resources. To use the CLI, install the Python package and call `python -m titan` from the command line.

The CLI allows you to `plan` and `apply` a Titan Core YAML config. You can specify a single input file or a directory of configs.

In addition to `plan` and `apply`, the CLI also allows you to `export` resources. This makes it easy to generate a config for an existing Snowflake environment.

To connect with Snowflake, the CLI uses environment variables. These environment variables are supported:

* `SNOWFLAKE_ACCOUNT`
* `SNOWFLAKE_USER`
* `SNOWFLAKE_PASSWORD`
* `SNOWFLAKE_DATABASE`
* `SNOWFLAKE_SCHEMA`
* `SNOWFLAKE_ROLE`
* `SNOWFLAKE_WAREHOUSE`
* `SNOWFLAKE_MFA_PASSCODE`

### CLI Example

```sh
# Show the help message
python -m titan --help

# Usage: python -m titan [OPTIONS] COMMAND [ARGS]...
# 
#   titan core helps you manage your Snowflake environment.
# 
# Options:
#   --help  Show this message and exit.
# 
# Commands:
#   apply   Apply a plan to Titan resources
#   export  Export Titan resources
#   plan    Generate an execution plan based on your configuration

# The CLI uses YAML config. This command creates a sample config file.

cat <<EOF > titan.yml
roles:
  - name: transformer

warehouses:
  - name: transforming
    warehouse_size: LARGE
    auto_suspend: 60

grants:
  - to_role: transformer
    priv: usage
    on_warehouse: transforming
EOF

# Set connection variables
export SNOWFLAKE_ACCOUNT="my-account"
export SNOWFLAKE_USER="my-user"
export SNOWFLAKE_PASSWORD="my-password"

# Generate a plan
python -m titan plan --config titan.yml

# Apply the config
python -m titan apply --config titan.yml
```

### Using the GitHub Action
The Titan Core GitHub Action allows you to automate the deployment of Snowflake resources using a git-based workflow.

### GitHub Action Example

```yaml
# .github/workflows/titan.yml
name: Titan Snowflake
on:
  push:
    branches: ["main"]
    # The directory in your repo where titan configs live.
    paths:
    - 'envs/prod/**'


jobs:
  deploy:
    runs-on: ubuntu-latest
    name: Deploy to Snowflake with Titan

    # The Github environment to use
    environment: prod
    steps:
      - uses: actions/checkout@v4
      - name: Deploy with Titan
        id: titan-core-action
        uses: Titan-Systems/titan-core-action@main
        with:
          resource-path: envs/prod
          valid-resource-types: database,user,warehouse,role
        env:
          SNOWFLAKE_ACCOUNT: ${{ secrets.SNOWFLAKE_ACCOUNT }}
          SNOWFLAKE_USERNAME: ${{ secrets.SNOWFLAKE_USERNAME }}
          SNOWFLAKE_PASSWORD: ${{ secrets.SNOWFLAKE_PASSWORD }}
          SNOWFLAKE_ROLE: ${{ secrets.SNOWFLAKE_ROLE }}
          SNOWFLAKE_WAREHOUSE: ${{ secrets.SNOWFLAKE_WAREHOUSE }}
```