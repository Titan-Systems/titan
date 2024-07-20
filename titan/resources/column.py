from dataclasses import dataclass

from ..enums import DataType, ResourceType
from ..parse import _parse_column, _parse_props
from ..props import FlagProp, Props, StringProp, TagsProp
from ..resource_tags import ResourceTags
from ..scope import TableScope
from .resource import Resource, ResourceSpec


@dataclass(unsafe_hash=True)
class _Column(ResourceSpec):
    name: str
    data_type: str
    collate: str = None
    comment: str = None
    not_null: bool = False
    constraint: str = None
    default: str = None
    tags: ResourceTags = None

    def __post_init__(self):
        super().__post_init__()
        try:
            self.data_type = DataType(self.data_type).value
        except ValueError:
            self.data_type = self.data_type.upper()


class Column(Resource):
    """
    <col_name> <col_type>
      [ COLLATE '<collation_specification>' ]
      [ COMMENT '<string_literal>' ]
      [ { DEFAULT <expr>
        | { AUTOINCREMENT | IDENTITY } [ { ( <start_num> , <step_num> ) | START <num> INCREMENT <num> } ] } ]
      [ NOT NULL ]
      [ [ WITH ] MASKING POLICY <policy_name> [ USING ( <col_name> , <cond_col1> , ... ) ] ]
      [ [ WITH ] TAG ( <tag_name> = '<tag_value>' [ , <tag_name> = '<tag_value>' , ... ] ) ]
      [ inlineConstraint ]

    inlineConstraint ::=
      [ CONSTRAINT <constraint_name> ]
      { UNIQUE | PRIMARY KEY | { [ FOREIGN KEY ] REFERENCES <ref_table_name> [ ( <ref_col_name> ) ] } }
      [ <constraint_properties> ]
    """

    resource_type = ResourceType.COLUMN
    props = Props(
        collate=StringProp("collate", eq=False),
        comment=StringProp("comment", eq=False),
        not_null=FlagProp("not null"),
        tags=TagsProp(),
    )
    scope = TableScope()
    spec = _Column
    serialize_inline = True

    def __init__(
        self,
        name: str,
        data_type: DataType,
        collate: str = None,
        comment: str = None,
        not_null: bool = False,
        constraint: str = None,
        default: str = None,
        tags: dict[str, str] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._data: _Column = _Column(
            name=name,
            data_type=data_type,
            collate=collate,
            comment=comment,
            not_null=not_null,
            constraint=constraint,
            default=default,
            tags=tags,
        )

    @classmethod
    def from_sql(cls, sql):
        parse_results = _parse_column(sql)
        remainder = parse_results.pop("remainder", "")
        props = _parse_props(cls.props, remainder)
        return cls(**parse_results, **props)
