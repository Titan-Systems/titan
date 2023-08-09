from pydantic import BaseModel, ConfigDict, Field

from .parse import FullyQualifiedIdentifier


class FQN(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    database: str = None
    schema_: str = Field(alias="schema", default=None)
    name: str

    @classmethod
    def from_str(cls, fqn_str, resource_key=None):
        parts = FullyQualifiedIdentifier().parse_string(fqn_str).as_list()
        if len(parts) == 1:
            return cls(name=parts[0])
        elif len(parts) == 2:
            if resource_key in ["schema"]:
                return cls(database=parts[0], name=parts[1])
            else:
                return cls(schema=parts[0], name=parts[1])
        elif len(parts) == 3:
            return cls(database=parts[0], schema=parts[1], name=parts[2])
        raise Exception(f"Invalid FQN string: {fqn_str}")

    def __str__(self):
        db = f"{self.database}." if self.database else ""
        schema = f"{self.schema_}." if self.schema_ else ""
        return f"{db}{schema}{self.name}"


class URN(BaseModel):
    account: str
    resource_key: str
    fqn: FQN

    def __str__(self):
        return f"urn:{self.account}:{self.resource_key}/{self.fqn}"

    @classmethod
    def from_str(cls, urn_str):
        parts = urn_str.split(":")
        if len(parts) != 3:
            raise Exception(f"Invalid URN string: {urn_str}")
        if parts[0] != "urn":
            raise Exception(f"Invalid URN string: {urn_str}")
        account = parts[1]
        resource_key, fqn_str = parts[2].split("/")
        fqn = FQN.from_str(fqn_str, resource_key=resource_key)
        return cls(account=account, resource_key=resource_key, fqn=fqn)

    @classmethod
    def from_resource(cls, account, resource):
        return cls(account=account, resource_key=resource.resource_key, fqn=resource.fqn)
