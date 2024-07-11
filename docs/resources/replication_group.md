---
description: >-
  
---

# ReplicationGroup

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/create-replication-group)

A replication group in Snowflake.


## Examples

### Python

```python
replication_group = ReplicationGroup(
    name="some_replication_group",
    object_types=["DATABASES"],
    allowed_accounts=["account1", "account2"],
)
```


### YAML

```yaml
replication_groups:
  - name: some_replication_group
    object_types:
      - DATABASES
    allowed_accounts:
      - account1
      - account2
```


## Fields

* `name` (string, required) - The name of the replication group.
* `object_types` (list, required) - The object types to be replicated.
* `allowed_accounts` (list, required) - The accounts allowed to replicate.
* `allowed_databases` (list) - The databases allowed to replicate.
* `allowed_shares` (list) - The shares allowed to replicate.
* `allowed_integration_types` (list) - The integration types allowed to replicate.
* `ignore_edition_check` (bool) - Whether to ignore the edition check.
* `replication_schedule` (string) - The replication schedule.
* `owner` (string or [Role](role.md)) - The owner of the replication group. Defaults to "SYSADMIN".


