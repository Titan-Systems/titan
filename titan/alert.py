from .resource import Resource
from .props import Props, StringProp, QueryProp, AlertConditionProp


class Alert(Resource):
    """
    CREATE [ OR REPLACE ] ALERT [ IF NOT EXISTS ] <name>
      WAREHOUSE = <warehouse_name>
      SCHEDULE = '{ <num> MINUTE | USING CRON <expr> <time_zone> }'
      COMMENT = '<string_literal>'
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
        condition=AlertConditionProp(),
        then=QueryProp("then"),
    )

    name: str
    owner: str = None
    warehouse: str = None
    schedule: str = None
    comment: str = None
    condition: str = None
    then: str = None
