from typing import Union, Optional

from .entity import AccountLevelEntity


class User(AccountLevelEntity):
    """ """

    def __init__(
        self,
        **kwargs,
    ):
        super().__init__(**kwargs)
