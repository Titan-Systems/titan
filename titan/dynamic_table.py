import re

from typing import Optional, Type, Union

from .resource import SchemaLevelResource, ResourceWithDB
from .warehouse import Warehouse
from .props import (
    Identifier,
    EnumProp,
    QueryProp,
    StringProp,
    IdentifierProp,
)


class DynamicTable(SchemaLevelResource, metaclass=ResourceWithDB):
    """
    CREATE [ OR REPLACE ] DYNAMIC TABLE <name>
      TARGET_LAG = { '<num> { seconds | minutes | hours | days }' | DOWNSTREAM }
      WAREHOUSE = <warehouse_name>
      AS <query>
    """

    resource_name = "DYNAMIC TABLE"

    ownable = True

    create_statement = re.compile(
        rf"""
            CREATE\s+
            (?:OR\s+REPLACE\s+)?
            DYNAMIC\s+TABLE\s+
            ({Identifier.pattern})
        """,
        re.IGNORECASE | re.VERBOSE,
    )

    props = {
        "TARGET_LAG": StringProp("TARGET_LAG", alt_tokens=["DOWNSTREAM"]),
        "WAREHOUSE": IdentifierProp("WAREHOUSE"),
        "AS_": QueryProp("AS"),
    }

    def __init__(
        self, target_lag: Optional[str] = None, warehouse: Union[None, str, Warehouse] = None, as_: str = "", **kwargs
    ):
        super().__init__(**kwargs)
        self.target_lag = target_lag
        if warehouse is None:
            raise ValueError("warehouse must be specified")
        self.warehouse = Warehouse.all[warehouse] if isinstance(warehouse, str) else warehouse
        self.as_ = as_

    # @classmethod
    # def from_sql(cls, sql: str):
    #     match = re.search(cls.create_statement, sql)

    #     if match is None:
    #         raise Exception

    #     name = match.group(1)
    #     file_format_class, props = cls.parse_anonymous_file_format(sql[match.end() :])
    #     return file_format_class(name=name, **props)
