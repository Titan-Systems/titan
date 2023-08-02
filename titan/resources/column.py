import pyparsing as pp

from ..enums import DataType
from ..props import Props, StringProp
from ..parse import Identifier, ANY


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
        comment=StringProp("comment"),
    )

    name: str
    type: DataType
    comment: str = None

    @classmethod
    def from_sql(cls, sql):
        parser = (Identifier | pp.dbl_quoted_string) + ANY
        for (name, type), start, end in parser.scan_string(sql):
            remainder = sql[end:]
            props = cls.props.parse(remainder)
            return cls(name=name, type=DataType.parse(type), **props)
