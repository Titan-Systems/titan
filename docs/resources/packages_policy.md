---
description: >-
  
---

# PackagesPolicy

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/create-packages-policy.html)

A Packages Policy defines a set of rules for allowed and blocked packages
that can be applied to various operations within the system.


## Examples

### Python

```python
packages_policy = PackagesPolicy(
    name="some_packages_policy",
    allowlist=["numpy", "pandas"],
    blocklist=["os", "sys"],
    comment="Policy for data processing packages."
)
```


### YAML

```yaml
packages_policy:
  - name: some_packages_policy
    allowlist:
      - numpy
      - pandas
    blocklist:
      - os
      - sys
    comment: Policy for data processing packages.
```


## Fields

* `name` (string, required) - The name of the packages policy.
* `language` (string or [Language](language.md)) - The programming language for the packages. Defaults to PYTHON.
* `allowlist` (list) - A list of package specifications that are explicitly allowed.
* `blocklist` (list) - A list of package specifications that are explicitly blocked.
* `additional_creation_blocklist` (list) - A list of package specifications that are blocked during creation.
* `comment` (string) - A comment or description for the packages policy.
* `owner` (string or [Role](role.md)) - The owner role of the packages policy. Defaults to SYSADMIN.


