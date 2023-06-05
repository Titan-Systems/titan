from typing import Union, Optional

from .resource import DatabaseLevelResource


class Schema(DatabaseLevelResource):
    def future_tables(self):
        pass


def parse_schema(str_or_schema: Union[None, str, Schema]) -> Optional[Schema]:
    if isinstance(str_or_schema, Schema):
        return str_or_schema
    elif type(str_or_schema) == str:
        return Schema(name=str_or_schema)
    else:
        return None
