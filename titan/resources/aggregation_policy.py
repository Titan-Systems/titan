from dataclasses import dataclass

from .resource import Resource, ResourceSpec, ResourceNameTrait
from .role import Role
from ..enums import ResourceType
from ..resource_name import ResourceName
from ..scope import SchemaScope
from ..enums import AccountEdition

from ..props import (
    Props,
    QueryProp,
    StringProp,
)


@dataclass(unsafe_hash=True)
class _AggregationPolicy(ResourceSpec):
    name: ResourceName
    body: str
    # comment: str = None
    owner: Role = "SYSADMIN"


# TODO:
# Aggregation policies have a weird construction and may require new prop types to handle comment
# CREATE [ OR REPLACE ] AGGREGATION POLICY [ IF NOT EXISTS ] <name>
#   AS () RETURNS AGGREGATION_CONSTRAINT -> <body>
#   [ COMMENT = '<string_literal>' ]


class AggregationPolicy(ResourceNameTrait, Resource):
    """
    Description:
        Represents an aggregation policy in Snowflake, which defines constraints on aggregation operations.

    Snowflake Docs:
        https://docs.snowflake.com/en/sql-reference/sql/create-aggregation-policy

    Fields:
        name (string, required): The name of the aggregation policy.
        body (string, required): The SQL expression defining the aggregation constraint.
        owner (string or Role): The owner of the aggregation policy. Defaults to "SYSADMIN".

    Python:

        ```python
        aggregation_policy = AggregationPolicy(
            name="some_aggregation_policy",
            body="AGGREGATION_CONSTRAINT(MIN_GROUP_SIZE => 5)",
            owner="SYSADMIN"
        )
        ```

    Yaml:

        ```yaml
        aggregation_policies:
          - name: some_aggregation_policy
            body: AGGREGATION_CONSTRAINT(MIN_GROUP_SIZE => 5)
            owner: SYSADMIN
        ```

    """

    edition = {AccountEdition.ENTERPRISE, AccountEdition.BUSINESS_CRITICAL}
    resource_type = ResourceType.AGGREGATION_POLICY
    props = Props(
        _start_token="AS () RETURNS AGGREGATION_CONSTRAINT",
        body=QueryProp("->"),
    )
    scope = SchemaScope()
    spec = _AggregationPolicy

    def __init__(
        self,
        name: str,
        body: str,
        owner: str = "SYSADMIN",
        **kwargs,
    ):
        super().__init__(name, **kwargs)
        self._data: _AggregationPolicy = _AggregationPolicy(
            name=self._name,
            body=body,
            owner=owner,
        )
