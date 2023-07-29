from enum import Enum


class Scope(Enum):
    ORGANIZATION = "ORGANIZATION"
    ACCOUNT = "ACCOUNT"
    DATABASE = "DATABASE"
    SCHEMA = "SCHEMA"
    TABLE = "TABLE"
