---
description: >-
  
---

# ComputePool

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/create-compute-pool)

A compute pool is a group of compute resources in Snowflake that can be used to execute SQL queries.


## Examples

### Python

```python
compute_pool = ComputePool(
    name="some_compute_pool",
    owner="ACCOUNTADMIN",
    min_nodes=2,
    max_nodes=10,
    instance_family="CPU_X64_S",
    auto_resume=True,
    initially_suspended=False,
    auto_suspend_secs=1800,
    comment="Example compute pool"
)
```


### YAML

```yaml
compute_pools:
  - name: some_compute_pool
    owner: ACCOUNTADMIN
    min_nodes: 2
    max_nodes: 10
    instance_family: CPU_X64_S
    auto_resume: true
    initially_suspended: false
    auto_suspend_secs: 1800
    comment: Example compute pool
```


## Fields

* `name` (string, required) - The unique name of the compute pool.
* `owner` (string or [Role](role.md)) - The owner of the compute pool. Defaults to "ACCOUNTADMIN".
* `min_nodes` (int) - The minimum number of nodes in the compute pool.
* `max_nodes` (int) - The maximum number of nodes in the compute pool.
* `instance_family` (string) - The family of instances to use for the compute nodes.
* `auto_resume` (bool) - Whether the compute pool should automatically resume when queries are submitted. Defaults to True.
* `initially_suspended` (bool) - Whether the compute pool should start in a suspended state.
* `auto_suspend_secs` (int) - The number of seconds of inactivity after which the compute pool should automatically suspend. Defaults to 3600.
* `comment` (string) - An optional comment about the compute pool.


