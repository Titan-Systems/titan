from typing import Optional, Union

from .enums import ResourceType

from .resource_name import ResourceName


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


class FQN:
    def __init__(
        self,
        name: str,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        arg_types: Optional[list] = None,
        params: Optional[dict] = None,
    ) -> None:
        self.name = name
        self.database = database
        self.schema = schema
        self.arg_types = arg_types or []
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
                ResourceName(self.name),
                ResourceName(self.database) if self.database else None,
                ResourceName(self.schema) if self.schema else None,
                tuple(self.arg_types),
                tuple(
                    self.params.items(),
                ),
            )
        )

    def __str__(self):
        db = f"{ResourceName(self.database)}." if self.database else ""
        schema = f"{ResourceName(self.schema)}." if self.schema else ""
        arg_types = f"({', '.join(self.arg_types)})" if self.arg_types else ""
        params = "?" + _params_to_str(self.params) if self.params else ""
        return f"{db}{schema}{self.name}{arg_types}{params}"

    def __repr__(self):  # pragma: no cover
        db = f", db={self.database}" if self.database else ""
        schema = f", schema={self.schema}" if self.schema else ""
        params = "?" + _params_to_str(self.params) if self.params else ""
        return f"FQN(name={self.name}{db}{schema}{params})"


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

    def __init__(self, resource_type: ResourceType, fqn: FQN, account_locator: str) -> None:
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

    # @classmethod
    # def from_locator(cls, locator: "ResourceLocator"):
    #     if locator.star:
    #         raise Exception("Cannot create URN from a wildcard locator")
    #     return cls(resource_type=locator.resource_key, fqn=FQN.from_str(locator.locator))

    @classmethod
    def from_session_ctx(cls, session_ctx):
        return cls(
            resource_type=ResourceType.ACCOUNT,
            fqn=FQN(name=session_ctx["account"]),
            account_locator=session_ctx["account_locator"],
        )

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


class ResourceLocator:
    """
    ResourceLocator

    A simple query language for locating resources within a Snowflake account.
    """

    def __init__(self, resource_key: str, locator: str) -> None:
        self.resource_key = resource_key
        self.locator = locator
        self.star = self.locator == "*"

    @classmethod
    def from_str(cls, resource_str: str) -> "ResourceLocator":
        """
        Parse a resource locator string.

        Usage
        -----
        Locate all resources:
        >>> ResourceLocator.from_str("*")

        Locate a specific resource:
        >>> ResourceLocator.from_str("database:mydb")
        >>> ResourceLocator.from_str("schema:mydb.my_schema")
        >>> ResourceLocator.from_str("table:mydb.my_schema.my_table")

        Locate all resources of a given type:
        >>> ResourceLocator.from_str("database:*")

        Locate all resources within a given scope:
        >>> ResourceLocator.from_str("database:mydb.*")
        """

        if resource_str == "*":
            return cls(resource_key="account", locator="*")

        parts = resource_str.split(":")
        if len(parts) != 2:
            raise Exception(f"Invalid resource locator string: {resource_str}")
        return cls(resource_key=parts[0], locator=parts[1])

    def __str__(self):
        return f"{self.resource_key}:{self.locator}"

    def __repr__(self):  # pragma: no cover
        return f"ResourceLocator(resource_key='{self.resource_key}', locator='{self.locator}')"
