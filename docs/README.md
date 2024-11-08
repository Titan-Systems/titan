# `titan core` - Snowflake infrastructure as code

Titan Core helps you provision, deploy, and secure resources in Snowflake. It replaces tools like Terraform, Schemachange, or Permifrost.

Deploy any Snowflake resource, including users, roles, schemas, databases, integrations, pipes, stages, functions, stored procedures, and more. Convert adhoc, bug-prone SQL management scripts into simple, repeatable configuration.

## Titan Core is for

* DevOps engineers looking to automate and manage Snowflake infrastructure.
* Analytics engineers working with dbt who want to manage Snowflake resources without macros.
* Data platform teams who need to reliably manage Snowflake with CI/CD.
* Organizations that prefer a git-based workflow for infrastructure management.
* Teams seeking to replace Terraform for Snowflake-related tasks.


## Key Features

 * **Declarative** » Generates the right SQL to make your config and account match

 * **Comprehensive** » Nearly every Snowflake resource is supported

 * **Flexible** » Write resource configuration in YAML or Python

 * **Fast** » Titan Core runs 50-90% faster than Terraform and Permifrost

 * **Migration-friendly** » Generate config automatically with the export CLI

## Contents

* [Getting Started](getting-started.md): Installation and initial setup guide.
* [Blueprint](blueprint.md): Customize and control how resources are deployed to Snowflake
* [GitHub Action](titan-core-github-action.md) - For git-based workflows, including dbt.

