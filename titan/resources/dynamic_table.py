from titan.props import Props, StringProp, IdentifierProp, QueryProp

from ..resource import Resource, SchemaScoped
from .warehouse import Warehouse


class DynamicTable(Resource, SchemaScoped):
    """
    CREATE [ OR REPLACE ] DYNAMIC TABLE <name>
      TARGET_LAG = { '<num> { seconds | minutes | hours | days }' | DOWNSTREAM }
      WAREHOUSE = <warehouse_name>
      AS <query>
    """

    resource_type = "DYNAMIC TABLE"
    props = Props(
        target_lag=StringProp("target_lag", alt_tokens=["DOWNSTREAM"]),
        warehouse=IdentifierProp("warehouse", Warehouse),
        as_=QueryProp("as"),
    )

    name: str
    owner: str = None
    target_lag: str
    warehouse: Warehouse
    as_: str
