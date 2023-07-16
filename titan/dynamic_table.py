from .props import Props, StringProp, IdentifierProp, QueryProp

from .resource2 import Resource, Namespace


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
        warehouse=IdentifierProp("WAREHOUSE"),
        as_=QueryProp("AS"),
    )

    name: str
    owner: str = None
    target_lag: str = None
    warehouse: str = None
    as_: str = None
