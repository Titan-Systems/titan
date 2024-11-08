# `titan core` GitHub Action

## Using the GitHub action

To add the Titan Core GitHub action to your repository, follow these steps:

### Create a Titan workflow file

Create a file in the GitHub workflows directory of your repo (`.github/workflows/titan.yml`)

```YAML
-- .github/workflows/titan.yml
name: Deploy to Snowflake with Titan
on:
  push:
    branches: [ main ]
    paths:
    - 'titan/**'

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout code
        uses: actions/checkout@v4

      - name: Deploy to Snowflake
        uses: Titan-Systems/titan-core-action@main
        with:
          run-mode: 'create-or-update'
          resource-path: './titan'
          allowlist: 'warehouse,role,grant'
          dry-run: 'false'
        env:
          SNOWFLAKE_ACCOUNT: ${{ secrets.SNOWFLAKE_ACCOUNT }}
          SNOWFLAKE_USER: ${{ secrets.SNOWFLAKE_USER }}
          SNOWFLAKE_PASSWORD: ${{ secrets.SNOWFLAKE_PASSWORD }}
          SNOWFLAKE_ROLE: ${{ secrets.SNOWFLAKE_ROLE }}
          SNOWFLAKE_WAREHOUSE: ${{ secrets.SNOWFLAKE_WAREHOUSE }}
```

### Configure your Snowflake connection

Go to your GitHub repository settings, navigate to `Secrets`. There, add a secret for `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, and whatever other connection settings you need.


### Create a `titan` directory in your repository

Add YAML resource configs to the `titan` directory.

```YAML
# titan/warehouses.yml
warehouses:
  - name: reporting
    warehouse_size: XSMALL
    auto_suspend: 60
    auto_resume: true
```

```YAML
# titan/rbac.yml

roles:
  - name: reporter
    comment: "Has permissions on the analytics database..."

grants:
  - to_role: reporter
    priv: usage
    on_warehouse: reporting
  - to_role: reporter
    priv: usage
    on_database: analytics

role_grants:
  - role: reporter
    roles:
      - SYSADMIN
```

### Commit and push your changes

When you push to `main` changes to files in the `titan/` directory, the Github Action will deploy them to Snowflake.

## Configuration options

**run-mode** `string`

Defines how the blueprint interacts with the Snowflake account

- Default: `"create-or-update"`
- **create-or-update**
  - Resources are either created or updated, no resources are destroyed
- **sync**:
  - `⚠️ WARNING` Sync mode will drop resources.
  - Titan will update Snowflake to match the blueprint exactly. Must be used with `allowlist`.

**resource-path** `string`

Defines the file or directory where Titan will look for the resource configs

- Default: `"."`

**allowlist** `list[string] or "all"`

Defines which resource types are allowed 

 - Default: `"all"`

**dry_run** `bool`

**vars** `dict`

**vars_spec** `list[dict]`

**scope** `str`

**database** `str`

**schema** `str`

## Ignore files with `.titanignore`

If you specify a directory as the `resource-path`, Titan will recursively look for all files with a `.yaml` or `.yml` file extension. You can tell Titan to exclude files or directories with a `.titanignore` file. This file uses [gitignore syntax](https://git-scm.com/docs/gitignore).

### `.titanignore` example

```
# .titanignore

# Ignore dbt config
dbt_project.yml
```