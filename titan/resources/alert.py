from typing import Dict

from .base import Resource, SchemaScoped
from .warehouse import T_Warehouse
from ..builder import tidy_sql
from ..props import Props, StringProp, QueryProp, AlertConditionProp, TagsProp


class Alert(Resource, SchemaScoped):
    """
    CREATE [ OR REPLACE ] ALERT [ IF NOT EXISTS ] <name>
      WAREHOUSE = <warehouse_name>
      SCHEDULE = '{ <num> MINUTE | USING CRON <expr> <time_zone> }'
      COMMENT = '<string_literal>'
      [ [ WITH ] TAG ( <tag_name> = '<tag_value>' [ , <tag_name> = '<tag_value>' , ... ] ) ]
      IF( EXISTS(
        <condition>
      ))
      THEN
        <action>
    """

    resource_type = "ALERT"
    props = Props(
        warehouse=StringProp("warehouse"),
        schedule=StringProp("schedule"),
        comment=StringProp("comment"),
        tags=TagsProp(),
        condition=AlertConditionProp(),
        then=QueryProp("then"),
    )

    name: str
    owner: str = "SYSADMIN"
    warehouse: T_Warehouse
    schedule: str
    comment: str = None
    tags: Dict[str, str] = None
    condition: str
    then: str

    def create_sql(self, or_replace=False, if_not_exists=False):
        return tidy_sql(
            "CREATE",
            "OR REPLACE" if or_replace else "",
            self.resource_type,
            "IF NOT EXISTS" if if_not_exists else "",
            self.fqn,
            self.props.render(self),
        )
