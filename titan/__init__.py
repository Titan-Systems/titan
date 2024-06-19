import logging.config

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
