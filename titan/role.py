from typing import Union, Optional

from .entity import AccountLevelEntity

from .grant import UsageGrant

# from .helpers import ParsableEnum


class Role(AccountLevelEntity):
    """ """

    def __init__(
        self,
        **kwargs,
    ):
        super().__init__(**kwargs)
        # self.data_retention_time_in_days = data_retention_time_in_days
        # self.max_data_extension_time_in_days = max_data_extension_time_in_days
        # self.default_ddl_collation = default_ddl_collation
        # self.comment = comment

    def uses(self, *entities):
        grants = []
        for ent in entities:
            grant = UsageGrant(self, ent)
            grants.append(grant)
        return grants

    def owns(self, *entities):
        for ent in entities:
            ent.owner = self
