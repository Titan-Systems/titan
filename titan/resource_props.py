from .props import BoolProp, EnumProp, Props, IntProp, StringProp, TagsProp, FlagProp

schema_props = Props(
    transient=FlagProp("transient"),
    managed_access=FlagProp("with managed access"),
    data_retention_time_in_days=IntProp("data_retention_time_in_days"),
    max_data_extension_time_in_days=IntProp("max_data_extension_time_in_days"),
    default_ddl_collation=StringProp("default_ddl_collation"),
    tags=TagsProp(),
    comment=StringProp("comment"),
)
