import pyparsing as pp

from ..enums import DataType
from ..props import FlagProp, Props, StringProp
from ..parse import COLUMN, _parse_props, _scan


from . import Resource


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
      [ , <col_name> <col_type> [ ... ] ]
      [ , outoflineConstraint ]
      [ , ... ]

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

    name: str
    type: DataType
    collate: str = None
    comment: str = None
    not_null: bool = None

    @classmethod
    def from_sql(cls, sql):
        parse_results, start, end = _scan(COLUMN, sql)
        col_name = parse_results["col_name"]
        col_type = DataType.parse(parse_results["col_type"])
        remainder = sql[end:]
        props = _parse_props(cls.props, remainder)
        return cls(name=col_name, type=col_type, **props)
