from typing import Dict

from titan.props import IntProp, StringProp, TagsProp, FlagProp, Props

from .dynamic_table import DynamicTable

from .file_format import FileFormat
from .pipe import Pipe
from .stage import Stage
from .table import Table
from .view import View

from titan.resource import Resource, Namespace, ResourceDB, DatabaseScoped


class Schema(Resource, DatabaseScoped):
    """
    CREATE [ OR REPLACE ] [ TRANSIENT ] SCHEMA [ IF NOT EXISTS ] <name>
      [ CLONE <source_schema>
            [ { AT | BEFORE } ( { TIMESTAMP => <timestamp> | OFFSET => <time_difference> | STATEMENT => <id> } ) ] ]
      [ WITH MANAGED ACCESS ]
      [ DATA_RETENTION_TIME_IN_DAYS = <integer> ]
      [ MAX_DATA_EXTENSION_TIME_IN_DAYS = <integer> ]
      [ DEFAULT_DDL_COLLATION = '<collation_specification>' ]
      [ [ WITH ] TAG ( <tag_name> = '<tag_value>' [ , <tag_name> = '<tag_value>' , ... ] ) ]
      [ COMMENT = '<string_literal>' ]
    """

    resource_type = "SCHEMA"
    namespace = Namespace.DATABASE
    props = Props(
        transient=FlagProp("transient"),
        with_managed_access=FlagProp("with managed access"),
        data_retention_time_in_days=IntProp("data_retention_time_in_days"),
        max_data_extension_time_in_days=IntProp("max_data_extension_time_in_days"),
        default_ddl_collation=StringProp("default_ddl_collation"),
        tags=TagsProp(),
        comment=StringProp("comment"),
    )

    name: str
    transient: bool = False
    owner: str = None
    with_managed_access: bool = False
    data_retention_time_in_days: int = None
    max_data_extension_time_in_days: int = None
    default_ddl_collation: str = None
    tags: Dict[str, str] = None
    comment: str = None

    _dynamic_tables: ResourceDB
    _file_formats: ResourceDB
    _pipes: ResourceDB
    _stages: ResourceDB
    _tables: ResourceDB
    _views: ResourceDB

    def model_post_init(self, ctx):
        super().model_post_init(ctx)

        self._dynamic_tables = ResourceDB(DynamicTable)
        self._file_formats = ResourceDB(FileFormat)
        self._pipes = ResourceDB(Pipe)
        # self._sprocs = ResourceDB(Sproc)
        self._stages = ResourceDB(Stage)
        self._tables = ResourceDB(Table)
        self._views = ResourceDB(View)

    @property
    def dynamic_tables(self):
        return self._dynamic_tables

    @property
    def file_formats(self):
        return self._file_formats

    @property
    def pipes(self):
        return self._pipes

    @property
    def sprocs(self):
        return self._sprocs

    @property
    def stages(self):
        return self._stages

    @property
    def tables(self):
        return self._tables

    @property
    def views(self):
        return self._views

    def add(self, *other_resources: Resource):
        for other_resource in other_resources:
            if other_resource.namespace and other_resource.namespace != Namespace.SCHEMA:
                raise TypeError(f"Cannot add {other_resource} to {self}")
            if isinstance(other_resource, View):
                self.views[other_resource.name] = other_resource
            else:
                raise TypeError(f"Cannot add {other_resource} to {self}")
            other_resource.schema = self
