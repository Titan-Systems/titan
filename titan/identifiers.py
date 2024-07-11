from typing import Optional, Union

import pyparsing as pp

from .enums import ResourceType
from .parse_primitives import FullyQualifiedIdentifier
from .resource_name import ResourceName


class FQN:
    def __init__(
        self,
        name: ResourceName,
        database: Optional[ResourceName] = None,
        schema: Optional[ResourceName] = None,
        arg_types: Optional[list] = None,
        params: Optional[dict] = None,
    ) -> None:

        if not isinstance(name, ResourceName):
            raise TypeError(f"FQN name: {name} is {type(name)}, not a ResourceName")

        if database and not isinstance(database, ResourceName):
            raise TypeError(f"FQN database: {database} is {type(database)}, not a ResourceName")

        if schema and not isinstance(schema, ResourceName):
            raise TypeError(f"FQN schema: {schema} is {type(schema)}, not a ResourceName")

        self.name = name
        self.database = database
        self.schema = schema
        self.arg_types = arg_types
        self.params = params or {}

    def __eq__(self, other):
        if not isinstance(other, FQN):
            return False
        return (
            names_are_equal(self.name, other.name)
            and names_are_equal(self.database, other.database)
            and names_are_equal(self.schema, other.schema)
            and self.arg_types == other.arg_types
            and self.params == other.params
        )

    def __hash__(self):
        return hash(
            (
                self.name,
                self.database,
                self.schema,
                tuple(self.arg_types or []),
                tuple(self.params.items()),
            )
        )

    def __str__(self):
        db = f"{self.database}." if self.database else ""
        schema = f"{self.schema}." if self.schema else ""
        arg_types = ""
        if self.arg_types is not None:
            arg_types = f"({', '.join(map(str, self.arg_types))})"
        params = "?" + _params_to_str(self.params) if self.params else ""
        return f"{db}{schema}{self.name}{arg_types}{params}"

    def __repr__(self):  # pragma: no cover

        name = getattr(self, "name", None)
        database = getattr(self, "database", None)
        schema = getattr(self, "schema", None)

        db = f", db={database}" if database else ""
        schema = f", schema={schema}" if schema else ""
        arg_types = ""
        if self.arg_types is not None:
            arg_types = f", args=({', '.join(map(str, self.arg_types))})"
        params = " ?" + _params_to_str(self.params) if self.params else ""
        return f"FQN(name={name}{db}{schema}{arg_types}{params})"


class URN:
    """
    Universal Resource Name

    An address scheme for uniquely identifying resources within a Snowflake account.

    Format
    ------

                     Resource
              Account  Type         Resource
          Org     │     │            Name     Params
        ───┴── ───┴── ──┴──        ───┴───── ───┴───────
    urn:ABC123:XYZ987:table/db.sch.sometable?param=value
                            ───┬────────────
                             Fully Qualified Name
    """

    def __init__(self, resource_type: ResourceType, fqn: FQN, account_locator: str = "") -> None:
        if not isinstance(resource_type, ResourceType):
            raise Exception(f"Invalid resource type: {resource_type}")
        self.resource_type: ResourceType = resource_type
        self.resource_label: str = resource_label_for_type(resource_type)
        self.fqn: FQN = fqn
        self.account_locator: str = account_locator
        self.organization: str = ""

    def __eq__(self, other):
        if not isinstance(other, URN):
            return False
        return (
            self.resource_type == other.resource_type
            and self.fqn == other.fqn
            and self.account_locator == other.account_locator
        )

    def __hash__(self):
        return hash((self.resource_type, self.fqn, self.account_locator))

    def __str__(self):
        return f"urn:{self.organization}:{self.account_locator}:{self.resource_label}/{self.fqn}"

    def __repr__(self):  # pragma: no cover
        org = getattr(self, "organization", "")
        acct = getattr(self, "account_locator", "")
        label = getattr(self, "resource_label", "")
        fqn = getattr(self, "fqn", "")
        return f"URN(urn:{org}:{acct}:{label}/{fqn})"

    @classmethod
    def from_resource(cls, resource, account_locator: str = ""):
        return cls(resource_type=resource.resource_type, fqn=resource.fqn, account_locator=account_locator)

    def database(self):
        if not self.fqn.database:
            raise Exception(f"URN does not have a database: {self}")
        return URN(
            resource_type=ResourceType.DATABASE,
            account_locator=self.account_locator,
            fqn=FQN(name=self.fqn.database),
        )

    def schema(self):
        if not self.fqn.schema:
            raise Exception(f"URN does not have a schema: {self}")
        return URN(
            resource_type=ResourceType.SCHEMA,
            account_locator=self.account_locator,
            fqn=FQN(name=self.fqn.schema, database=self.fqn.database),
        )


# class ResourceLocator:
#     """
#     ResourceLocator

#     A simple query language for locating resources within a Snowflake account.
#     """

#     def __init__(self, resource_key: str, locator: str) -> None:
#         self.resource_key = resource_key
#         self.locator = locator
#         self.star = self.locator == "*"

#     @classmethod
#     def from_str(cls, resource_str: str) -> "ResourceLocator":
#         """
#         Parse a resource locator string.

#         Usage
#         -----
#         Locate all resources:
#         >>> ResourceLocator.from_str("*")

#         Locate a specific resource:
#         >>> ResourceLocator.from_str("database:mydb")
#         >>> ResourceLocator.from_str("schema:mydb.my_schema")
#         >>> ResourceLocator.from_str("table:mydb.my_schema.my_table")

#         Locate all resources of a given type:
#         >>> ResourceLocator.from_str("database:*")

#         Locate all resources within a given scope:
#         >>> ResourceLocator.from_str("database:mydb.*")
#         """

#         if resource_str == "*":
#             return cls(resource_key="account", locator="*")

#         parts = resource_str.split(":")
#         if len(parts) != 2:
#             raise Exception(f"Invalid resource locator string: {resource_str}")
#         return cls(resource_key=parts[0], locator=parts[1])

#     def __str__(self):
#         return f"{self.resource_key}:{self.locator}"

#     def __repr__(self):  # pragma: no cover
#         return f"ResourceLocator(resource_key='{self.resource_key}', locator='{self.locator}')"


def parse_identifier(identifier: str, is_db_scoped=False) -> dict:
    # TODO: This needs to support periods and question marks in double quoted identifiers
    scoped_name, param_str = identifier.split("?") if "?" in identifier else (identifier, "")
    params = {}
    if param_str:
        for param in param_str.split("&"):
            k, v = param.split("=")
            params[k] = v

    arg_types = None
    if "(" in scoped_name:
        args_start = scoped_name.find("(")
        scoped_name, args_str = scoped_name[:args_start], scoped_name[args_start:]
        arg_types = [arg.strip() for arg in args_str.strip("()").split(",")]

    try:
        name_parts = list(FullyQualifiedIdentifier.parse_string(scoped_name, parse_all=True))
    except pp.ParseException:
        raise pp.ParseException(f"Failed to parse identifier: {identifier}")
    if len(name_parts) == 1:
        return {
            "name": name_parts[0],
            "params": params,
            "arg_types": arg_types,
        }
    elif len(name_parts) == 2:
        if is_db_scoped:
            return {
                "database": name_parts[0],
                "name": name_parts[1],
                "params": params,
                "arg_types": arg_types,
            }
        else:
            return {
                "schema": name_parts[0],
                "name": name_parts[1],
                "params": params,
                "arg_types": arg_types,
            }
    elif len(name_parts) == 3:
        return {
            "database": name_parts[0],
            "schema": name_parts[1],
            "name": name_parts[2],
            "params": params,
            "arg_types": arg_types,
        }
    elif len(name_parts) == 4:
        params["entity"] = name_parts[3]
        return {
            "database": name_parts[0],
            "schema": name_parts[1],
            "name": name_parts[2],
            "params": params,
            "arg_types": arg_types,
        }
    raise Exception(f"Failed to parse identifier: {identifier}")


def parse_FQN(fqn_str: str, is_db_scoped=False) -> FQN:
    identifier = parse_identifier(fqn_str, is_db_scoped=is_db_scoped)
    name = identifier.pop("name")
    database = identifier.pop("database", None)
    schema = identifier.pop("schema", None)
    return FQN(
        name=ResourceName(name),
        database=ResourceName(database) if database else None,
        schema=ResourceName(schema) if schema else None,
        **identifier,
    )


# NOTE: can't put this into identifiers.py:URN because of circular import
def parse_URN(urn_str: str) -> URN:
    parts = urn_str.split(":")
    if len(parts) != 4:
        raise Exception(f"Invalid URN string: {urn_str}")
    if parts[0] != "urn":
        raise Exception(f"Invalid URN string: {urn_str}")
    resource_label, fqn_str = parts[3].split("/", 1)
    resource_type = resource_type_for_label(resource_label)
    fqn = parse_FQN(fqn_str, is_db_scoped=(resource_label == "schema"))
    return URN(
        account_locator=parts[2],
        resource_type=resource_type,
        fqn=fqn,
    )


def _params_to_str(params: dict) -> str:
    return "&".join([f"{k.lower()}={v}" for k, v in params.items()])


def resource_label_for_type(resource_type: ResourceType) -> str:
    return str(resource_type).replace(" ", "_").lower()


def resource_type_for_label(resource_label: str) -> ResourceType:
    return ResourceType(resource_label.upper().replace("_", " "))


def names_are_equal(name1: Union[None, str, ResourceName], name2: Union[None, str, ResourceName]) -> bool:
    if name1 is None and name2 is None:
        return True
    if name1 is None or name2 is None:
        return False
    return ResourceName(name1) == ResourceName(name2)
