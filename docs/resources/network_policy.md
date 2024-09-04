---
description: >-
  
---

# NetworkPolicy

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/create-network-policy)

A Network Policy in Snowflake defines a set of network rules and IP addresses
that are allowed or blocked from accessing a Snowflake account. This helps in
managing network traffic and securing access based on network policies.


## Examples

### Python

```python
network_policy = NetworkPolicy(
    name="some_network_policy",
    allowed_network_rule_list=[NetworkRule(name="rule1"), NetworkRule(name="rule2")],
    blocked_network_rule_list=[NetworkRule(name="rule3")],
    allowed_ip_list=["192.168.1.1", "192.168.1.2"],
    blocked_ip_list=["10.0.0.1"],
    comment="Example network policy"
)
```


### YAML

```yaml
network_policies:
  - name: some_network_policy
    allowed_network_rule_list:
      - rule1
      - rule2
    blocked_network_rule_list:
      - rule3
    allowed_ip_list: ["192.168.1.1", "192.168.1.2"]
    blocked_ip_list: ["10.0.0.1"]
    comment: "Example network policy"
```


## Fields

* `name` (string, required) - The name of the network policy.
* `allowed_network_rule_list` (list) - A list of allowed network rules.
* `blocked_network_rule_list` (list) - A list of blocked network rules.
* `allowed_ip_list` (list) - A list of allowed IP addresses.
* `blocked_ip_list` (list) - A list of blocked IP addresses.
* `comment` (string) - A comment about the network policy.
* `owner` (string or [Role](role.md)) - The owner role of the network policy. Defaults to "SECURITYADMIN".


