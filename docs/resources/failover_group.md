---
description: >-
  
---

# FailoverGroup

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/create-failover-group)

Represents a failover group in Snowflake, which is a collection of databases, shares, and other resources
that can be failed over together to a secondary account in case of a disaster recovery scenario.


## Examples

### Python

```python
failover_group = FailoverGroup(
    name="some_failover_group",
    object_types=["DATABASES", "ROLES"],
    allowed_accounts=["org1.account1", "org2.account2"],
    allowed_databases=["db1", "db2"],
    allowed_shares=["share1", "share2"],
    allowed_integration_types=["SECURITY INTEGRATIONS", "API INTEGRATIONS"],
    ignore_edition_check=True,
    replication_schedule="USING CRON 0 0 * * * UTC",
    owner="ACCOUNTADMIN"
)
```


### YAML

```yaml
failover_groups:
  - name: some_failover_group
    object_types:
      - DATABASES
      - ROLES
    allowed_accounts:
      - org1.account1
      - org2.account2
    allowed_databases:
      - db1
      - db2
    allowed_shares:
      - share1
      - share2
    allowed_integration_types:
      - SECURITY INTEGRATIONS
      - API INTEGRATIONS
    ignore_edition_check: true
    replication_schedule: "USING CRON 0 0 * * * UTC"
    owner: ACCOUNTADMIN
```


## Fields

* `name` (string, required) - The name of the failover group.
* `object_types` (list) - The types of objects included in the failover group. Can include string or ObjectType.
* `allowed_accounts` (list, required) - The accounts that are allowed to be part of the failover group.
* `allowed_databases` (list) - The databases that are allowed to be part of the failover group.
* `allowed_shares` (list) - The shares that are allowed to be part of the failover group.
* `allowed_integration_types` (list) - The integration types that are allowed in the failover group. Can include string or IntegrationTypes.
* `ignore_edition_check` (bool) - Specifies whether to ignore the edition check. Defaults to None.
* `replication_schedule` (string) - The schedule for replication. Defaults to None.
* `owner` (string or [Role](role.md)) - The owner role of the failover group. Defaults to "ACCOUNTADMIN".


