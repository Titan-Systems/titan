---
description: >-
  
---

# AuthenticationPolicy

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/create-authentication-policy)

Defines the rules and constraints for authentication within the system, ensuring they meet specific security standards.


## Examples

### Python

```python
authentication_policy = AuthenticationPolicy(
    name="some_authentication_policy",
    authentication_methods=["PASSWORD", "SAML"],
    mfa_authentication_methods=["PASSWORD"],
    mfa_enrollment="REQUIRED",
    client_types=["SNOWFLAKE_UI"],
    security_integrations=["ALL"],
    comment="Policy for secure authentication."
)
```


### YAML

```yaml
authentication_policies:
  - name: some_authentication_policy
    authentication_methods:
      - PASSWORD
      - SAML
    mfa_authentication_methods:
      - PASSWORD
    mfa_enrollment: REQUIRED
    client_types:
      - SNOWFLAKE_UI
    security_integrations:
      - ALL
    comment: Policy for secure authentication.
```


## Fields

* `name` (string, required) - The name of the authentication policy.
* `authentication_methods` (list) - A list of allowed authentication methods.
* `mfa_authentication_methods` (list) - A list of authentication methods that enforce multi-factor authentication (MFA).
* `mfa_enrollment` (string) - Determines whether a user must enroll in multi-factor authentication. Defaults to OPTIONAL.
* `client_types` (list) - A list of clients that can authenticate with Snowflake.
* `security_integrations` (list) - A list of security integrations the authentication policy is associated with.
* `comment` (string) - A comment or description for the authentication policy.
* `owner` (string or [Role](role.md)) - The owner role of the authentication policy. Defaults to SECURITYADMIN.


