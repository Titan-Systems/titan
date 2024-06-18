import logging.config

from .blueprint import Blueprint
from .resources import *

logger = logging.getLogger("titan")


__all__ = [
    "Blueprint",
]

__version__ = "0.5.3"

LOGO = r"""
    __  _ __          
   / /_(_) /____  ___ 
  / __/ / __/ _ `/ _ \
  \__/_/\__/\_,_/_//_/
   

""".strip(
    "\n"
)
