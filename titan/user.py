from typing import Union, Optional

from .resource import AccountLevelResource


class User(AccountLevelResource):
    """ """

    def __init__(
        self,
        **kwargs,
    ):
        super().__init__(**kwargs)
