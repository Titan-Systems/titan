---
description: >-
  
---

# GlueCatalogIntegration

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/create-catalog-integration)

Manages the integration of AWS Glue as a catalog in Snowflake, supporting the ICEBERG table format.


## Examples

### Python

```python
glue_catalog_integration = GlueCatalogIntegration(
    name="some_catalog_integration",
    table_format="ICEBERG",
    glue_aws_role_arn="arn:aws:iam::123456789012:role/SnowflakeAccess",
    glue_catalog_id="some_glue_catalog_id",
    catalog_namespace="some_namespace",
    enabled=True,
    glue_region="us-west-2",
    comment="Integration for AWS Glue with Snowflake."
)
```


### YAML

```yaml
catalog_integrations:
  - name: some_catalog_integration
    table_format: ICEBERG
    glue_aws_role_arn: arn:aws:iam::123456789012:role/SnowflakeAccess
    glue_catalog_id: some_glue_catalog_id
    catalog_namespace: some_namespace
    enabled: true
    glue_region: us-west-2
    comment: Integration for AWS Glue with Snowflake.
```


## Fields

* `name` (string, required) - The name of the catalog integration.
* `table_format` (string or [CatalogTableFormat](catalog_table_format.md), required) - The format of the table, defaults to ICEBERG.
* `glue_aws_role_arn` (string, required) - The ARN for the AWS role to assume.
* `glue_catalog_id` (string, required) - The Glue catalog ID.
* `catalog_namespace` (string, required) - The namespace of the catalog.
* `enabled` (bool, required) - Specifies whether the catalog integration is enabled.
* `glue_region` (string) - The AWS region of the Glue catalog. Defaults to None.
* `owner` (string or [Role](role.md)) - The owner role of the catalog integration. Defaults to "ACCOUNTADMIN".
* `comment` (string) - An optional comment describing the catalog integration.


