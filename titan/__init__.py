import logging.config

# __version__ = open("version.md", encoding="utf-8").read().split(" ")[2]

from .blueprint import Blueprint
from .resources import *

logger = logging.getLogger("titan")


__all__ = [
    "Blueprint",
]

LOGO = r"""
    __  _ __          
   / /_(_) /____  ___ 
  / __/ / __/ _ `/ _ \
  \__/_/\__/\_,_/_//_/
   

""".strip(
    "\n"
)
