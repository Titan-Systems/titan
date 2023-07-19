from .props import Props, StringProp, IdentifierProp, QueryProp

from .resource import Resource, Namespace
from .warehouse import Warehouse


class DynamicTable(Resource):
    """
    CREATE [ OR REPLACE ] DYNAMIC TABLE <name>
      TARGET_LAG = { '<num> { seconds | minutes | hours | days }' | DOWNSTREAM }
      WAREHOUSE = <warehouse_name>
      AS <query>
    """

    resource_type = "DYNAMIC TABLE"
    namespace = Namespace.SCHEMA
    props = Props(
        target_lag=StringProp("TARGET_LAG", alt_tokens=["DOWNSTREAM"]),
        warehouse=IdentifierProp("WAREHOUSE", Warehouse),
        as_=QueryProp("AS"),
    )

    name: str
    owner: str = None
    target_lag: str = None
    warehouse: Warehouse = None
    as_: str = None
