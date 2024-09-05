from dataclasses import dataclass

from ..enums import ResourceType
from ..props import IntProp, Props, StringProp
from ..resource_name import ResourceName
from ..role_ref import RoleRef
from ..scope import SchemaScope
from .resource import NamedResource, Resource, ResourceSpec


@dataclass(unsafe_hash=True)
class _Sequence(ResourceSpec):
    name: ResourceName
    owner: RoleRef = "SYSADMIN"
    start: int = None
    increment: int = None
    comment: str = None


class Sequence(NamedResource, Resource):
    """
    Description:
        Manages the creation and configuration of sequences in Snowflake, which are objects that generate numeric values according to a specified sequence.

    Snowflake Docs:
        https://docs.snowflake.com/en/sql-reference/sql/create-sequence

    Fields:
        name (string, required): The name of the sequence.
        owner (string or Role): The owner role of the sequence. Defaults to "SYSADMIN".
        start (int): The starting value of the sequence.
        increment (int): The value by which the sequence is incremented.
        comment (string): A comment for the sequence.

    Python:

        ```python
        sequence = Sequence(
            name="some_sequence",
            owner="SYSADMIN",
            start=100,
            increment=10,
            comment="This is a sample sequence."
        )
        ```

    Yaml:

        ```yaml
        sequences:
          - name: some_sequence
            owner: SYSADMIN
            start: 100
            increment: 10
            comment: This is a sample sequence.
        ```
    """

    resource_type = ResourceType.SEQUENCE
    props = Props(
        _start_token="with",
        start=IntProp("start", consume=["with", "="], eq=False),
        increment=IntProp("increment", consume=["by", "="], eq=False),
        comment=StringProp("comment"),
    )
    scope = SchemaScope()
    spec = _Sequence

    def __init__(
        self,
        name: str,
        owner: str = "SYSADMIN",
        start: int = None,
        increment: int = None,
        comment: str = None,
        **kwargs,
    ):
        super().__init__(name, **kwargs)
        self._data: _Sequence = _Sequence(
            name=self._name,
            owner=owner,
            start=start,
            increment=increment,
            comment=comment,
        )
