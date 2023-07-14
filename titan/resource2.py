from typing import Union, Optional, List, Tuple, Dict, ClassVar

from pydantic import BaseModel, ConfigDict


class Props:
    def __init__(self, **props):
        self.all = props


class Resource(BaseModel):
    resource_name: ClassVar[str]
    ownable: ClassVar[bool]
    props: ClassVar[Props]

    @classmethod
    def from_sql(self, sql):
        import pyparsing as pp

        return sql
