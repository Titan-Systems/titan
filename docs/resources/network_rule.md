---
description: >-
  
---

# NetworkRule

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/create-network-rule)

A Network Rule in Snowflake defines a set of network addresses, such as IP addresses or hostnames,
that can be allowed or denied access to a Snowflake account. This helps in managing network traffic
and securing access based on network policies.


## Examples

### Python

```python
network_rule = NetworkRule(
    name="some_network_rule",
    type="IPV4",
    value_list=["192.168.1.1", "192.168.1.2"],
    mode="INGRESS",
    comment="Example network rule"
)
network_rule = NetworkRule(
    name="some_network_rule",
    database="somedb",
    schema="someschema",
    type="IPV4",
    value_list=["192.168.1.1", "192.168.1.2"],
    mode="INGRESS",
    comment="Example network rule with fully qualified name"
)
```


### YAML

```yaml
network_rules:
  - name: some_network_rule
    type: IPV4
    value_list: ["192.168.1.1", "192.168.1.2"]
    mode: INGRESS
    comment: "Example network rule"
  - name: some_network_rule
    database: somedb
    schema: someschema
    type: IPV4
    value_list: ["192.168.1.1", "192.168.1.2"]
    mode: INGRESS
    comment: "Example network rule with fully qualified name"
```


## Fields

* `name` (string, required) - The name of the network rule.
* `type` (string or [NetworkIdentifierType](network_identifier_type.md), required) - The type of network identifier. Defaults to IPV4.
* `value_list` (list) - A list of values associated with the network rule.
* `mode` (string or [NetworkRuleMode](network_rule_mode.md)) - The mode of the network rule. Defaults to INGRESS.
* `comment` (string) - A comment about the network rule.
* `owner` (string or [Role](role.md)) - The owner role of the network rule. Defaults to "SYSADMIN".


