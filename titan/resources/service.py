from dataclasses import dataclass

from .resource import Resource, ResourceSpec
from .compute_pool import ComputePool
from .warehouse import Warehouse
from ..enums import ResourceType
from ..resource_name import ResourceName
from ..scope import SchemaScope
from ..props import (
    StringProp,
    IdentifierProp,
    BoolProp,
    IntProp,
    TagsProp,
    Props,
    IdentifierListProp,
)


@dataclass(unsafe_hash=True)
class _ServiceSpec(ResourceSpec):
    name: ResourceName
    compute_pool: ComputePool
    stage: str = None
    yaml_file_stage_path: str = None
    specification_text: str = None
    external_access_integrations: list[str] = None
    auto_resume: bool = True
    min_instances: int = None
    max_instances: int = None
    query_warehouse: Warehouse = None
    tags: dict[str, str] = None
    comment: str = None


class Service(Resource):
    """Service is a managed service that runs on a compute pool in Snowflake

    CREATE SERVICE [ IF NOT EXISTS ] <name>
      IN COMPUTE POOL <compute_pool_name>
      {
        FROM @<stage>
        SPECIFICATION_FILE = '<yaml_file_stage_path>'
        |
        FROM SPECIFICATION <specification_text>
      }
      [ EXTERNAL_ACCESS_INTEGRATIONS = ( <EAI_name> [ , ... ] ) ]
      [ AUTO_RESUME = { TRUE | FALSE } ]
      [ MIN_INSTANCES = <num> ]
      [ MAX_INSTANCES = <num> ]
      [ QUERY_WAREHOUSE = <warehouse_name> ]
      [ [ WITH ] TAG ( <tag_name> = '<tag_value>' [ , <tag_name> = '<tag_value>' , ... ] ) ]
      [ COMMENT = '{string_literal}']
    """

    resource_type = ResourceType.SERVICE
    props = Props(
        compute_pool=IdentifierProp("in compute pool"),
        # stage=StringProp("stage"),
        specification_file=StringProp("specification_file"),
        specification_text=StringProp("from specification"),
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
        specification_text: str = None,
        external_access_integrations: list[str] = None,
        auto_resume: bool = True,
        min_instances: int = None,
        max_instances: int = None,
        query_warehouse: Warehouse = None,
        tags: dict[str, str] = None,
        comment: str = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._data = _ServiceSpec(
            name=name,
            compute_pool=compute_pool,
            stage=stage,
            yaml_file_stage_path=yaml_file_stage_path,
            specification_text=specification_text,
            external_access_integrations=external_access_integrations,
            auto_resume=auto_resume,
            min_instances=min_instances,
            max_instances=max_instances,
            query_warehouse=query_warehouse,
            tags=tags,
            comment=comment,
        )
