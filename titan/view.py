import re

from typing import List, Optional, Tuple

from .resource import SchemaLevelResource
from .props import Identifier, BoolProp, QueryProp, StringProp, IntProp, TagsProp, IdentifierProp


class View(SchemaLevelResource):
    """
    CREATE [ OR REPLACE ] [ SECURE ] [ { [ { LOCAL | GLOBAL } ] TEMP | TEMPORARY | VOLATILE } ] [ RECURSIVE ] VIEW [ IF NOT EXISTS ] <name>
      [ ( <column_list> ) ]
      [ <col1> [ WITH ] MASKING POLICY <policy_name> [ USING ( <col1> , <cond_col1> , ... ) ]
               [ WITH ] TAG ( <tag_name> = '<tag_value>' [ , <tag_name> = '<tag_value>' , ... ] ) ]
      [ , <col2> [ ... ] ]
      [ [ WITH ] ROW ACCESS POLICY <policy_name> ON ( <col_name> [ , <col_name> ... ] ) ]
      [ [ WITH ] TAG ( <tag_name> = '<tag_value>' [ , <tag_name> = '<tag_value>' , ... ] ) ]
      [ COPY GRANTS ]
      [ COMMENT = '<string_literal>' ]
      AS <select_statement>
    """

    resource_name = "VIEW"

    props = {
        "tags": TagsProp(),
        "copy_grants": BoolProp("COPY GRANTS"),
        "comment": StringProp("COMMENT"),
        "as_": QueryProp("AS"),
    }

    # class Props:
    #     tags = TagsProp()
    #     copy_grants = BoolProp("COPY GRANTS")
    #     comment = StringProp("COMMENT")
    #     as_ = QueryProp("AS")

    create_statement = re.compile(
        rf"""
            CREATE\s+
            (?:OR\s+REPLACE\s+)?
            (?:SECURE\s+)?
            VIEW\s+
            (?:IF\s+NOT\s+EXISTS\s+)?
            ({Identifier.pattern})
            """,
        re.IGNORECASE | re.VERBOSE,
    )

    ownable = True

    def __init__(
        self,
        name: str,
        tags: List[Tuple[str, str]] = [],
        copy_grants: Optional[bool] = None,
        comment: Optional[str] = None,
        as_: Optional[str] = None,
        **kwargs,
    ):
        super().__init__(name, **kwargs)
        self.tags = tags
        self.copy_grants = copy_grants
        self.comment = comment
        self.as_ = as_
