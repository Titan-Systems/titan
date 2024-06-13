from dataclasses import dataclass

from .resource import Resource, ResourceSpec
from .column import Column

from ..enums import ResourceType
from ..scope import SchemaScope
from ..props import (
    Props,
    SchemaProp,
    StringProp,
    TagsProp,
)


@dataclass(unsafe_hash=True)
class _HybridTable(ResourceSpec):
    name: str
    columns: list[Column]
    tags: dict[str, str] = None
    owner: Role = "SYSADMIN"
    comment: str = None

    def __post_init__(self):
        super().__post_init__()
        if self.columns is None:
            raise ValueError("columns can't be None")
        if len(self.columns) == 0:
            raise ValueError("columns can't be empty")


class HybridTable(Resource):
    resource_type = ResourceType.HYBRID_TABLE
    props = Props(
        columns=SchemaProp(),
        tags=TagsProp(),
        comment=StringProp("comment"),
    )
    scope = SchemaScope()
    spec = _HybridTable

    def __init__(
        self,
        name: str,
        columns: list[Column],
        tags: dict[str, str] = None,
        owner: str = "SYSADMIN",
        comment: str = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._data = _HybridTable(
            name=name,
            columns=columns,
            tags=tags,
            owner=owner,
            comment=comment,
        )

    @classmethod
    def from_sql(cls, sql):
        """
        CREATE [ OR REPLACE ] HYBRID TABLE [ IF NOT EXISTS ] <table_name>
          ( <col_name> <col_type>
            [
              {
                DEFAULT <expr>
                  /* AUTOINCREMENT (or IDENTITY) is supported only for numeric data types (NUMBER, INT, FLOAT, etc.) */
                | { AUTOINCREMENT | IDENTITY }
                  [
                    {
                      ( <start_num> , <step_num> )
                      | START <num> INCREMENT <num>
                    }
                  ]
                  [ { ORDER | NOORDER } ]
              }
            ]
            [ NOT NULL ]
            [ inlineConstraint ]
            [ , <col_name> <col_type> [ ... ] ]
            [ , outoflineIndex ]
            [ , ... ]
          )
          [ COMMENT = '<string_literal>' ]
        """

        raise NotImplementedError

        # identifier, remainder = _parse_create_header(sql, cls.resource_type, cls.scope)
        # table_schema, remainder = _parse_table_schema(remainder)
        # props = _parse_props(cls.props, remainder)
        # return cls(**identifier, **table_schema, **props)
