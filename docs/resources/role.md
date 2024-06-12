---
description: >-
  Roles are assigned to users to allow them to perform actions required for
  business functions in their organization.
---

# Role

### Example

#### Python

```
role = Role(
  name = "some_role",
  owner = "USERADMIN",
  comment = "Some role comment",
)
```

#### YAML

```yaml
roles:
  - name: some_role
    owner: USERADMIN
    comment: Some role comment
```

### Fields

* `name` (required) - Identifier for the role; must be unique for your account
* `owner` (string or Role) - The role that owns this resource
* `tags` (dict) - Tag declarations for this resource
* `comment` (string) - A comment for this resource

