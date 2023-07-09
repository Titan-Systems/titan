import re

from typing import List, Tuple, Optional, TYPE_CHECKING

from .resource import AccountLevelResource

from .props import Identifier, StringProp, TagsProp

if TYPE_CHECKING:
    from .grants import PrivGrant

# from .helpers import ParseableEnum


class Role(AccountLevelResource):
    """
    CREATE [ OR REPLACE ] ROLE [ IF NOT EXISTS ] <name>
      [ [ WITH ] TAG ( <tag_name> = '<tag_value>' [ , <tag_name> = '<tag_value>' , ... ] ) ]
      [ COMMENT = '<string_literal>' ]
    """

    resource_name = "ROLE"

    props = {
        "TAGS": TagsProp(),
        "COMMENT": StringProp("COMMENT"),
    }

    create_statement = re.compile(
        rf"""
            CREATE\s+
            (?:OR\s+REPLACE\s+)?
            ROLE\s+
            (?:IF\s+NOT\s+EXISTS\s+)?
            ({Identifier.pattern})
            """,
        re.IGNORECASE | re.VERBOSE,
    )

    def __init__(
        self,
        name: str,
        tags: List[Tuple[str, str]] = [],
        comment: Optional[str] = None,
        **kwargs,
    ):
        super().__init__(name, **kwargs)
        self.tags = tags
        self.comment = comment

    # def uses(self, *resources):
    #     grants = []
    #     for res in resources:
    #         grant = UsageGrant(self, res)
    #         grants.append(grant)
    #     return grants

    def grant(self, *grants: "PrivGrant"):
        for grant in grants:
            pass
            # grant.grantee = self

    def owns(self, *resources):
        for res in resources:
            res.owner = self
