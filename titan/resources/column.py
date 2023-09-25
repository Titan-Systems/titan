from typing_extensions import Annotated

from pydantic import BeforeValidator

from .base import Resource, ResourceName, _fix_class_documentation
from ..enums import DataType
from ..props import FlagProp, Props, StringProp
from ..parse import _parse_column, _parse_props


@_fix_class_documentation
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

      # [ , <col_name> <col_type> [ ... ] ]
      # [ , outoflineConstraint ]
      # [ , ... ]

    inlineConstraint ::=
      [ CONSTRAINT <constraint_name> ]
      { UNIQUE | PRIMARY KEY | { [ FOREIGN KEY ] REFERENCES <ref_table_name> [ ( <ref_col_name> ) ] } }
      [ <constraint_properties> ]

    outoflineConstraint ::=
      [ CONSTRAINT <constraint_name> ]
      {
         UNIQUE [ ( <col_name> [ , <col_name> , ... ] ) ]
       | PRIMARY KEY [ ( <col_name> [ , <col_name> , ... ] ) ]
       | [ FOREIGN KEY ] [ ( <col_name> [ , <col_name> , ... ] ) ]
                         REFERENCES <ref_table_name> [ ( <ref_col_name> [ , <ref_col_name> , ... ] ) ]
      }
      [ <constraint_properties> ]
    """

    resource_type = "COLUMN"
    props = Props(
        collate=StringProp("collate", eq=False),
        comment=StringProp("comment", eq=False),
        not_null=FlagProp("not null"),
    )

    name: ResourceName
    data_type: str  # DataType
    collate: str = None
    comment: str = None
    not_null: bool = None
    constraint: str = None

    @classmethod
    def from_sql(cls, sql):
        parse_results = _parse_column(sql)
        remainder = parse_results.pop("remainder", "")
        props = _parse_props(cls.props, remainder)
        return cls(**parse_results, **props)


def _coerce(sql_or_resource):
    if isinstance(sql_or_resource, str):
        return Column.from_sql(sql_or_resource)
    else:
        return sql_or_resource


T_Column = Annotated[Column, BeforeValidator(_coerce)]
