from dataclasses import dataclass, field

from ..enums import AccountEdition, ResourceType
from ..props import (
    BoolProp,
    IdentifierListProp,
    IdentifierProp,
    IntProp,
    Props,
    StringProp,
    TagsProp,
)
from ..resource_name import ResourceName
from ..scope import SchemaScope
from .compute_pool import ComputePool
from .resource import NamedResource, Resource, ResourceSpec
from .tag import TaggableResource
from .warehouse import Warehouse


@dataclass(unsafe_hash=True)
class _ServiceSpec(ResourceSpec):
    name: ResourceName
    compute_pool: ComputePool
    stage: str = field(default=None, metadata={"fetchable": False})
    yaml_file_stage_path: str = field(default=None, metadata={"fetchable": False})
    specification: str = field(default=None, metadata={"fetchable": False})
    external_access_integrations: list[str] = None
    auto_resume: bool = True
    min_instances: int = None
    max_instances: int = None
    query_warehouse: Warehouse = None
    comment: str = None


class Service(NamedResource, TaggableResource, Resource):
    """
    Description:
        Service is a managed resource in Snowflake that allows users to run instances of their applications
        as a collection of containers on a compute pool. Each service instance can handle incoming traffic
        with the help of a load balancer if multiple instances are run.

    Snowflake Docs:
        https://docs.snowflake.com/en/sql-reference/sql/create-service

    Fields:
        name (string, required): The unique identifier for the service within the schema.
        compute_pool (string or ComputePool, required): The compute pool on which the service runs.
        stage (string): The Snowflake internal stage where the specification file is stored.
        yaml_file_stage_path (string): The path to the service specification file on the stage.
        specification (string): The service specification as a string.
        external_access_integrations (list): The names of external access integrations for the service.
        auto_resume (bool): Specifies whether to automatically resume the service when a function or ingress is called. Defaults to True.
        min_instances (int): The minimum number of service instances to run.
        max_instances (int): The maximum number of service instances to run.
        query_warehouse (string or Warehouse): The warehouse to use if a service container connects to Snowflake to execute a query.
        tags (dict): Tags associated with the service.
        comment (string): A comment for the service.

    Python:

        ```python
        service = Service(
            name="some_service",
            compute_pool="some_compute_pool",
            stage="@tutorial_stage",
            yaml_file_stage_path="echo_spec.yaml",
            specification="FROM SPECIFICATION $$some_specification$$",
            external_access_integrations=["some_integration"],
            auto_resume=True,
            min_instances=1,
            max_instances=2,
            query_warehouse="some_warehouse",
            tags={"key": "value"},
            comment="This is a sample service."
        )
        ```

    Yaml:

        ```yaml
        services:
          - name: some_service
            compute_pool: some_compute_pool
            stage: @tutorial_stage
            yaml_file_stage_path: echo_spec.yaml
            specification: FROM SPECIFICATION $$some_specification$$
            external_access_integrations:
              - some_integration
            auto_resume: true
            min_instances: 1
            max_instances: 2
            query_warehouse: some_warehouse
            tags:
              key: value
            comment: This is a sample service.
        ```
    """

    edition = {AccountEdition.ENTERPRISE, AccountEdition.BUSINESS_CRITICAL}
    resource_type = ResourceType.SERVICE
    props = Props(
        compute_pool=IdentifierProp("in compute pool", eq=False),
        specification_file=StringProp("specification_file"),
        specification=StringProp("from specification", eq=False),
        external_access_integrations=IdentifierListProp("external_access_integrations", parens=True),
        auto_resume=BoolProp("auto_resume"),
        min_instances=IntProp("min_instances"),
        max_instances=IntProp("max_instances"),
        query_warehouse=IdentifierProp("query_warehouse"),
        tags=TagsProp(),
        comment=StringProp("comment"),
    )
    scope = SchemaScope()
    spec = _ServiceSpec

    def __init__(
        self,
        name: str,
        compute_pool: ComputePool,
        stage: str = None,
        yaml_file_stage_path: str = None,
        specification: str = None,
        external_access_integrations: list[str] = None,
        auto_resume: bool = True,
        min_instances: int = None,
        max_instances: int = None,
        query_warehouse: Warehouse = None,
        tags: dict[str, str] = None,
        comment: str = None,
        **kwargs,
    ):
        super().__init__(name, **kwargs)
        self._data: _ServiceSpec = _ServiceSpec(
            name=self._name,
            compute_pool=compute_pool,
            stage=stage,
            yaml_file_stage_path=yaml_file_stage_path,
            specification=specification,
            external_access_integrations=external_access_integrations,
            auto_resume=auto_resume,
            min_instances=min_instances,
            max_instances=max_instances,
            query_warehouse=query_warehouse,
            comment=comment,
        )
        self.set_tags(tags)
