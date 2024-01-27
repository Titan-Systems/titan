from dataclasses import dataclass

from .resource import Resource, ResourceSpec
from .warehouse import Warehouse
from ..enums import ResourceType
from ..scope import SchemaScope

from ..props import Props, StringProp, QueryProp, AlertConditionProp, TagsProp


@dataclass
class _Alert(ResourceSpec):
    name: str
    warehouse: Warehouse
    schedule: str
    condition: str
    then: str
    owner: str = "SYSADMIN"
    comment: str = None
    tags: dict[str, str] = None


class Alert(Resource):
    """Alerts trigger notifications when certain conditions are met.

    Args:
        name (str): The name of the alert.
        warehouse (str): The name of the warehouse to run the query on.
        schedule (str): The schedule for the alert to run on.
        condition (str): The condition for the alert to trigger on.
        then (str): The query to run when the alert triggers.
        owner (str, optional): The owner of the alert. Defaults to "SYSADMIN".
        comment (str, optional): A comment for the alert. Defaults to None.
        tags (dict[str, str], optional): Tags for the alert. Defaults to None.
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
        super().__init__(**kwargs)
        self._data = _Alert(
            name=name,
            warehouse=warehouse,
            schedule=schedule,
            condition=condition,
            then=then,
            owner=owner,
            comment=comment,
            tags=tags,
        )
        self.requires(self._data.warehouse)
