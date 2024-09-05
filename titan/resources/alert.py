from dataclasses import dataclass

from ..enums import ResourceType
from ..props import AlertConditionProp, Props, QueryProp, StringProp, TagsProp
from ..resource_name import ResourceName
from ..role_ref import RoleRef
from ..scope import SchemaScope
from .resource import NamedResource, Resource, ResourceSpec
from .tag import TaggableResource
from .warehouse import Warehouse


@dataclass(unsafe_hash=True)
class _Alert(ResourceSpec):
    name: ResourceName
    warehouse: Warehouse
    schedule: str
    condition: str
    then: str
    owner: RoleRef = "SYSADMIN"
    comment: str = None


class Alert(NamedResource, TaggableResource, Resource):
    """
    Description:
        Alerts trigger notifications when certain conditions are met.

    Snowflake Docs:
        https://docs.snowflake.com/en/sql-reference/sql/create-alert

    Fields:
        name (string, required): The name of the alert.
        warehouse (string or Warehouse): The name of the warehouse to run the query on.
        schedule (string): The schedule for the alert to run on.
        condition (string): The condition for the alert to trigger on.
        then (string): The query to run when the alert triggers.
        owner (string or Role): The owner role of the alert. Defaults to "SYSADMIN".
        comment (string): A comment for the alert. Defaults to None.
        tags (dict): Tags for the alert. Defaults to None.

    Python:

        ```python
        alert = Alert(
            name="some_alert",
            warehouse="some_warehouse",
            schedule="USING CRON * * * * *",
            condition="SELECT COUNT(*) FROM some_table",
            then="CALL SYSTEM$SEND_EMAIL('example@example.com', 'Alert Triggered', 'The alert condition was met.')",
        )
        ```

    Yaml:

        ```yaml
        alerts:
          - name: some_alert
            warehouse: some_warehouse
            schedule: USING CRON * * * * *
            condition: SELECT COUNT(*) FROM some_table
            then: CALL SYSTEM$SEND_EMAIL('example@example.com', 'Alert Triggered', 'The alert condition was met.')
        ```
    """

    resource_type = ResourceType.ALERT
    props = Props(
        warehouse=StringProp("warehouse"),
        schedule=StringProp("schedule"),
        comment=StringProp("comment"),
        tags=TagsProp(),
        condition=AlertConditionProp(),
        then=QueryProp("then"),
    )
    scope = SchemaScope()
    spec = _Alert

    def __init__(
        self,
        name: str,
        warehouse: Warehouse,
        schedule: str,
        condition: str,
        then: str,
        owner: str = "SYSADMIN",
        comment: str = None,
        tags: dict[str, str] = None,
        **kwargs,
    ):
        super().__init__(name, **kwargs)
        self._data: _Alert = _Alert(
            name=self._name,
            warehouse=warehouse,
            schedule=schedule,
            condition=condition,
            then=then,
            owner=owner,
            comment=comment,
        )
        self.set_tags(tags)
        self.requires(self._data.warehouse)
