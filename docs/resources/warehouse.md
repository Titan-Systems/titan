---
description: >-
  A virtual warehouse is a cluster of compute resources in Snowflake. It is used
  to execute SQL queries and load data.
---

# Warehouse

### Example

#### Python

```python
wh = Warehouse(
  name = "some_warehouse",
  owner = "SYSADMIN",
  warehouse_type = "standard",
  warehouse_size = "x-small",
  auto_suspend = 30,
  auto_resume = True,
  comment = "Some warehouse comment",
)
```

#### YAML

```yaml
warehouses:
  - name: some_warehouse
    owner: SYSADMIN
    warehouse_type: standard
    warehouse_size: x-small
    auto_suspend: 30
    auto_resume: true
    comment: Some warehouse comment
```

### Fields


* `name` (string, required) - Identifier for the virtual warehouse; must be unique for your account.
* `owner` (string or [Role](role.md)) - The role that owns this resource
* `warehouse_type` (string or [WarehouseType](warehouse.md#warehousetype))


### Enums

#### WarehouseType

* STANDARD
* SNOWPARK-OPTIMIZED
