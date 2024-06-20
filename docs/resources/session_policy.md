---
description: >-
  
---

# SessionPolicy

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/create-session-policy)

Manages session policies in Snowflake, which define timeout settings for user sessions to enhance security.


## Examples

### Python

```python
session_policy = SessionPolicy(
    name="some_session_policy",
    session_idle_timeout_mins=30,
    session_ui_idle_timeout_mins=10,
    comment="Policy for standard users."
)
```


### YAML

```yaml
session_policies:
  - name: some_session_policy
    session_idle_timeout_mins: 30
    session_ui_idle_timeout_mins: 10
    comment: Policy for standard users.
```


## Fields

* `name` (string, required) - The name of the session policy.
* `session_idle_timeout_mins` (int) - The maximum amount of time a session can remain idle before it is automatically terminated.
* `session_ui_idle_timeout_mins` (int) - The maximum amount of time a user interface session can remain idle before it is automatically terminated.
* `comment` (string) - A description or comment about the session policy.


