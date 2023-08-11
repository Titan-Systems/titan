from typing_extensions import Annotated

from pydantic import BeforeValidator

from ..props import Props, StringProp, IdentifierProp, QueryProp
from .base import Resource, SchemaScoped
from .validators import coerce_from_str
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
        warehouse=IdentifierProp("warehouse"),
        as_=QueryProp("as"),
    )

    name: str
    owner: str = "SYSADMIN"
    target_lag: str
    warehouse: Annotated[Warehouse, BeforeValidator(coerce_from_str(Warehouse))]
    as_: str
