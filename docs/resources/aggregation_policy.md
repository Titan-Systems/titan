---
description: >-
  
---

# AggregationPolicy

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/create-aggregation-policy)

Represents an aggregation policy in Snowflake, which defines constraints on aggregation operations.


## Examples

### Python

```python
aggregation_policy = AggregationPolicy(
    name="some_aggregation_policy",
    body="AGGREGATION_CONSTRAINT(MIN_GROUP_SIZE => 5)",
    owner="SYSADMIN"
)
```


### YAML

```yaml
aggregation_policies:
  - name: some_aggregation_policy
    body: AGGREGATION_CONSTRAINT(MIN_GROUP_SIZE => 5)
    owner: SYSADMIN
```


## Fields

* `name` (string, required) - The name of the aggregation policy.
* `body` (string, required) - The SQL expression defining the aggregation constraint.
* `owner` (string or [Role](role.md)) - The owner of the aggregation policy. Defaults to "SYSADMIN".


