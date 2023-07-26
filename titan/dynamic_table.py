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
        target_lag=StringProp("target_lag", alt_tokens=["DOWNSTREAM"]),
        warehouse=IdentifierProp("warehouse", Warehouse),
        as_=QueryProp("as"),
    )

    name: str
    owner: str = None
    target_lag: str = None
    warehouse: Warehouse = None
    as_: str = None
