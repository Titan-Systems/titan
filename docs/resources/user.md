---
description: >-
  A user in Snowflake.
---

# User

## Example

### Python

```python
user = User(
    name="some_user",
    owner="USERADMIN",
    email="some.user@example.com",
)
```

### YAML

```yaml
users:
  - name: some_user
    owner: USERADMIN
    email: some.user@example.com
```

## Fields

(str): [required] The name of the user.
owner (str): The owner of the user. Defaults to "USERADMIN".
password (str): The password of the user.
login_name (str): The login name of the user. Defaults to the name in uppercase.
display_name (str): The display name of the user. Defaults to the name in lowercase.
first_name (str): The first name of the user.
middle_name (str): The middle name of the user.
last_name (str): The last name of the user.
email (str): The email of the user.

* `name` (required) - Identifier for the virtual warehouse; must be unique for your account.
* `owner` (string or [Role](role.md)) - The role that owns this resource
* `warehouse_type` (string or [WarehouseType](warehouse.md#warehousetype)

## Enums

### WarehouseType

* STANDARD
* SNOWPARK-OPTIMIZED

