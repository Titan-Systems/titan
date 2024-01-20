from typing import Optional

from inflection import underscore


class FQN:
    def __init__(
        self,
        name: str,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        arg_types: Optional[list] = None,
        params: dict = {},
    ) -> None:
        self.name = name.upper()
        self.database = database
        self.schema = schema
        self.arg_types = arg_types
        self.params = params

    def __str__(self):
        db = f"{self.database}." if self.database else ""
        schema = f"{self.schema}." if self.schema else ""
        arg_types = f"({', '.join(self.arg_types)})" if self.arg_types else ""
        params = "?" + "&".join([f"{k.lower()}={v}" for k, v in self.params.items()]) if self.params else ""
        return f"{db}{schema}{self.name}{arg_types}{params}"

    def __repr__(self):
        db = f", db={self.database}" if self.database else ""
        schema = f", schema={self.schema}" if self.schema else ""
        return f"FQN(name={self.name}{db}{schema})"


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

    def __init__(self, resource_type: str, fqn: FQN, account_locator: str, organization: str = "") -> None:
        # self.resource_type = underscore(resource_type)
        self.resource_type = resource_type.replace(" ", "_").lower()
        self.fqn = fqn
        self.account_locator = account_locator
        self.organization = organization

    def __str__(self):
        return f"urn:{self.organization}:{self.account_locator}:{self.resource_type}/{self.fqn}"

    def __repr__(self):
        return f"URN(urn:{self.organization}:{self.account_locator}:{self.resource_type}/{self.fqn})"

    @classmethod
    def from_resource(cls, resource, **kwargs):
        return cls(resource_type=str(resource.resource_type), fqn=resource.fqn, **kwargs)

    # @classmethod
    # def from_locator(cls, locator: "ResourceLocator"):
    #     if locator.star:
    #         raise Exception("Cannot create URN from a wildcard locator")
    #     return cls(resource_type=locator.resource_key, fqn=FQN.from_str(locator.locator))

    @classmethod
    def from_session_ctx(cls, session_ctx):
        return cls(
            resource_type="account",
            fqn=FQN(name=session_ctx["account"]),
            account_locator=session_ctx["account_locator"],
        )

    def database(self):
        if not self.fqn.database:
            raise Exception(f"URN does not have a database: {self}")
        return URN(
            resource_type="database",
            account_locator=self.account_locator,
            fqn=FQN(name=self.fqn.database),
        )

    def schema(self):
        if not self.fqn.schema:
            raise Exception(f"URN does not have a schema: {self}")
        return URN(
            resource_type="schema",
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

    def __repr__(self):
        return f"ResourceLocator(resource_key='{self.resource_key}', locator='{self.locator}')"
