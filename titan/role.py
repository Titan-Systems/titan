from typing import Union, Optional

from .resource import AccountLevelResource

# from .grant import UsageGrant

# from .helpers import ParsableEnum


class Role(AccountLevelResource):
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

    # def uses(self, *resources):
    #     grants = []
    #     for res in resources:
    #         grant = UsageGrant(self, res)
    #         grants.append(grant)
    #     return grants

    def owns(self, *resources):
        for res in resources:
            res.owner = self
