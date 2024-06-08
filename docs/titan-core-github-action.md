# `[titan core]` GitHub Action

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

          - name: Run Titan Core Action
            uses: Titan-Systems/titan-core-action@v1
            with:
              run-mode: 'create-or-update'
              resource-path: './resources'
              valid-resource-types: 'table,view'
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

## Usage

The Titan Core GitHub Action allows you to manage Snowflake resources using GitOps principles. Configure the action by setting the following inputs:

- `run-mode`: Operation mode (e.g., `create-or-update`, `fully-managed`).
- `resource-path`: Path to your resource configuration files.
- `valid-resource-types`: Comma-separated list of resource types to manage.
- `dry-run`: Set to `true` for a dry run (no changes made).

## Examples

### Example 1: Basic Setup

This example demonstrates a basic setup to manage tables and views.

```yaml
name: Basic Titan Core Workflow
on: [push]

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout code
        uses: actions/checkout@v2

      - name: Run Titan Core Action
        uses: Titan-Systems/titan-core-action@v1
        with:
          run-mode: 'create-or-update'
          resource-path: './resources'
          valid-resource-types: 'table,view'
          dry-run: 'false'
        env:
          SNOWFLAKE_ACCOUNT: ${{ secrets.SNOWFLAKE_ACCOUNT }}
          SNOWFLAKE_USER: ${{ secrets.SNOWFLAKE_USER }}
          SNOWFLAKE_PASSWORD: ${{ secrets.SNOWFLAKE_PASSWORD }}
          SNOWFLAKE_ROLE: ${{ secrets.SNOWFLAKE_ROLE }}
          SNOWFLAKE_WAREHOUSE: ${{ secrets.SNOWFLAKE_WAREHOUSE }}
```