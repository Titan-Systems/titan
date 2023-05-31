from typing import List, Tuple

from .entity import Stage, SchemaLevelEntity
from .generator import EqualsProp, FlagProp


class Table(SchemaLevelEntity):
    """
    CREATE [ OR REPLACE ]
      [ { [ { LOCAL | GLOBAL } ] TEMP | TEMPORARY | VOLATILE | TRANSIENT } ]
      TABLE [ IF NOT EXISTS ] <table_name>
      ( <col_name> <col_type>
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
        [ , ... ] )
          [ CLUSTER BY ( <expr> [ , <expr> , ... ] ) ]
          [ STAGE_FILE_FORMAT = ( { FORMAT_NAME = '<file_format_name>'
                                   | TYPE = { CSV | JSON | AVRO | ORC | PARQUET | XML } [ formatTypeOptions ] } ) ]
          [ STAGE_COPY_OPTIONS = ( copyOptions ) ]
          [ DATA_RETENTION_TIME_IN_DAYS = <integer> ]
          [ MAX_DATA_EXTENSION_TIME_IN_DAYS = <integer> ]
          [ CHANGE_TRACKING = { TRUE | FALSE } ]
          [ DEFAULT_DDL_COLLATION = '<collation_specification>' ]
          [ COPY GRANTS ]
          [ [ WITH ] ROW ACCESS POLICY <policy_name> ON ( <col_name> [ , <col_name> ... ] ) ]
          [ [ WITH ] TAG ( <tag_name> = '<tag_value>' [ , <tag_name> = '<tag_value>' , ... ] ) ]
          [ COMMENT = '<string_literal>' ]

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

    def __init__(
        self,
        autoload: bool = False,
        data_retention_time_in_days: int = None,
        max_data_extension_time_in_days: int = None,
        change_tracking: bool = None,
        default_ddl_collation: str = None,
        copy_grants: bool = None,
        tags: List[Tuple[str, str]] = [],
        comment: str = None,
        **kwargs,
    ):
        super().__init__(**kwargs)

        # name: str, query_text=None, implicit=False

        # TODO: make this a changeable property that registers/deregisters the pipe when the flag is flipped
        self.autoload = autoload

        self.data_retention_time_in_days = data_retention_time_in_days
        self.max_data_extension_time_in_days = max_data_extension_time_in_days
        self.change_tracking = change_tracking
        self.default_ddl_collation = default_ddl_collation
        self.copy_grants = copy_grants
        self.tags = tags
        self.comment = comment

        self.table_stage = Stage(name=f"@%{self.name}", implicit=True)
        self.table_stage.depends_on(self)

    @property
    def sql(self):
        return f"""
            CREATE TABLE
                { self.fully_qualified_name() }
                { EqualsProp("DATA_RETENTION_TIME_IN_DAYS", self.data_retention_time_in_days) }
                { EqualsProp("MAX_DATA_EXTENSION_TIME_IN_DAYS", self.max_data_extension_time_in_days) }
                { EqualsProp("CHANGE_TRACKING", self.change_tracking) }
                { EqualsProp("DEFAULT_DDL_COLLATION", self.default_ddl_collation) }
                { FlagProp("COPY GRANTS", self.copy_grants) }
                { "TagsProp(self.tags)" }
                { EqualsProp("COMMENT", self.comment) }
        """
        # return (
        #     "CREATE TABLE "
        #     + self.fully_qualified_name()
        #     + self.equals_prop("data_retention_time_in_days")
        #     + self.flag_prop("copy_grants", "COPY GRANTS")
        # )

    # def create(self, session):
    #     super().create(session)
    #     if self.autoload:
    #         raise NotImplementedError
    # Needs a refactor via dependencies
    # # Does this need to be a pipe we refresh, or should we just call the COPY INTO command each time?
    # pipe = Pipe(
    #     sql=rf"""
    #     CREATE PIPE {self.name}_autoload_pipe
    #         AS
    #         COPY INTO {self.name}
    #         FROM {self.table_stage}
    #         FILE_FORMAT = (
    #             TYPE = CSV
    #             SKIP_HEADER = 1
    #             COMPRESSION = GZIP
    #             FIELD_OPTIONALLY_ENCLOSED_BY = '\042'
    #             NULL_IF = '\N'
    #             NULL_IF = 'NULL'
    #         )
    #     """,
    # )
    # pipe.create(session)
