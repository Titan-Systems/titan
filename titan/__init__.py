from .adapter import Adapter
from .blueprint import Blueprint

from .resources import *

# from . import (
#     Alert,
#     Database,
#     ExternalFunction,
#     FileFormat,
#     Role,
#     Schema,
#     Sequence,
#     Table,
#     Tag,
#     Task,
#     User,
#     View,
#     Warehouse,
# )


__all__ = [
    # "App",
    "Adapter",
    "Alert",
    "Blueprint",
    "Database",
    "ExternalFunction",
    "FileFormat",
    "Role",
    "Schema",
    "Sequence",
    "SharedDatabase",
    "Stream",
    "Table",
    "Tag",
    "Task",
    "User",
    "View",
    "Warehouse",
]

__version__ = "0.0.1"


LOGO = r"""
    __  _ __          
   / /_(_) /____  ___ 
  / __/ / __/ _ `/ _ \
  \__/_/\__/\_,_/_//_/
   

""".strip(
    "\n"
)


from .enums import ParseableEnum, SchemaPrivs

# _old_new = SchemaPrivs.__new__


# def test_new(cls, value):
#     print("ok")
#     # _old_new(cls, value)
#     # cls.__new__(cls, value)
#     # super(ParseableEnum, cls).__new__(cls, value)


# SchemaPrivs.__new__ = test_new
