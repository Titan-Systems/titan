# `titan core` GitHub Action

## Installation

To add the Titan Core GitHub Action to your repository, follow these steps:

1. **Create or modify the workflow file**: Add the following code to your `.github/workflows/titan.yml` file.

    ```yaml
    name: Titan Core Workflow
    on: [push]

    jobs:
      deploy:
        runs-on: ubuntu-latest
        steps:
          - name: Checkout code
            uses: actions/checkout@v2

          - name: Deploy to Snowflake with Titan
            uses: Titan-Systems/titan-core-action@main
            with:
              run-mode: 'create-or-update'
              resource-path: './resources'
              allowlist: 'warehouse,role,grant'
              dry-run: 'false'
            env:
              SNOWFLAKE_ACCOUNT: ${{ secrets.SNOWFLAKE_ACCOUNT }}
              SNOWFLAKE_USER: ${{ secrets.SNOWFLAKE_USER }}
              SNOWFLAKE_PASSWORD: ${{ secrets.SNOWFLAKE_PASSWORD }}
              SNOWFLAKE_ROLE: ${{ secrets.SNOWFLAKE_ROLE }}
              SNOWFLAKE_WAREHOUSE: ${{ secrets.SNOWFLAKE_WAREHOUSE }}
    ```

2. **Specify Snowflake secrets**: Go to your GitHub repository settings, navigate to `Secrets`, and add the following secrets:
    - `SNOWFLAKE_ACCOUNT`
    - `SNOWFLAKE_USER`
    - `SNOWFLAKE_PASSWORD`
    - `SNOWFLAKE_ROLE`
    - `SNOWFLAKE_WAREHOUSE`

3. Create a `resources` directory in your repository with your Snowflake resources.

```YAML
# resources/warehouses.yml
warehouses:
  - name: loading
    warehouse_size: XSMALL
    auto_suspend: 60
    auto_resume: true
  - name: transforming
    warehouse_size: XSMALL
    auto_suspend: 60
    auto_resume: true
  - name: reporting
    warehouse_size: XSMALL
    auto_suspend: 60
    auto_resume: true
```

```YAML
# resources/rbac.yml
# Specify roles
roles:
  - name: loader
    comment: "Owns the tables in your raw database, and connects to the loading warehouse."
  - name: transformer
    comment: "Has query permissions on tables in raw database and owns tables in the analytics database. This is for dbt developers and scheduled jobs."
  - name: reporter
    comment: "Has permissions on the analytics database only. This role is for data consumers, such as analysts and BI tools. These users will not have permissions to read data from the raw database."

# Permissions
grants:
  - to_role: loader
    priv: usage
    on_warehouse: loading
  - to_role: transformer
    priv: usage
    on_warehouse: transforming
  - to_role: reporter
    priv: usage
    on_warehouse: reporting
  - to_role: reporter
    priv: usage
    on_database: analytics

# Grant all roles to SYSADMIN
role_grants:
  - role: loader
    roles:
      - SYSADMIN
  - role: transformer
    roles:
      - SYSADMIN
  - role: reporter
    roles:
      - SYSADMIN
```

4. Commit and push your changes.