from typing import Optional

from pydantic import BaseModel, ConfigDict, Field


class FQN(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    database: str = None
    schema_: str = Field(alias="schema", default=None)
    name: str
    params: dict = {}

    @classmethod
    def from_str(cls, fqn_str, resource_key=None):
        # TODO: This needs to support periods and question marks in double quoted identifiers
        scoped_name, param_str = fqn_str.split("?") if "?" in fqn_str else (fqn_str, "")
        params = {}
        if param_str:
            for param in param_str.split("&"):
                k, v = param.split("=")
                params[k] = v
        name_parts = scoped_name.split(".")
        if len(name_parts) == 1:
            return cls(name=name_parts[0], params=params)
        elif len(name_parts) == 2:
            if resource_key in ["schema"]:
                return cls(database=name_parts[0], name=name_parts[1], params=params)
            else:
                return cls(schema=name_parts[0], name=name_parts[1], params=params)
        elif len(name_parts) == 3:
            return cls(database=name_parts[0], schema=name_parts[1], name=name_parts[2], params=params)
        raise Exception(f"Invalid FQN string: {fqn_str}")

    def __str__(self):
        db = f"{self.database}." if self.database else ""
        schema = f"{self.schema_}." if self.schema_ else ""
        params = "?" + "&".join([f"{k.lower()}={v}" for k, v in self.params.items()]) if self.params else ""
        return f"{db}{schema}{self.name}{params}"


class URN(BaseModel):
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

    account: str = ""
    organization: str = ""
    resource_key: str
    fqn: FQN

    def __str__(self):
        return f"urn:{self.organization}:{self.account}:{self.resource_key}/{self.fqn}"

    @classmethod
    def from_str(cls, urn_str):
        parts = urn_str.split(":")
        if len(parts) != 4:
            raise Exception(f"Invalid URN string: {urn_str}")
        if parts[0] != "urn":
            raise Exception(f"Invalid URN string: {urn_str}")
        resource_key, fqn_str = parts[3].split("/")
        fqn = FQN.from_str(fqn_str, resource_key=resource_key)
        return cls(
            organization=parts[1],
            account=parts[2],
            resource_key=resource_key,
            fqn=fqn,
        )

    @classmethod
    def from_resource(cls, resource, **kwargs):
        return cls(resource_key=resource.resource_key, fqn=resource.fqn, **kwargs)
