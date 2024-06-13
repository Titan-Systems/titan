from dataclasses import dataclass

from .resource import Resource, ResourceSpec
from .role import Role
from ..enums import ParseableEnum, ResourceType
from ..props import Props, EnumProp, StringProp, BoolProp
from ..resource_name import ResourceName
from ..scope import AccountScope


class CatalogSource(ParseableEnum):
    GLUE = "GLUE"
    OBJECT_STORE = "OBJECT_STORE"


class CatalogTableFormat(ParseableEnum):
    ICEBERG = "ICEBERG"


@dataclass(unsafe_hash=True)
class _GlueCatalogIntegration(ResourceSpec):
    name: ResourceName
    glue_aws_role_arn: str
    glue_catalog_id: str
    catalog_namespace: str
    enabled: bool
    catalog_source: CatalogSource = CatalogSource.GLUE
    table_format: CatalogTableFormat = CatalogTableFormat.ICEBERG
    glue_region: str = None
    owner: Role = "ACCOUNTADMIN"
    comment: str = None

    def __post_init__(self):
        super().__post_init__()
        if self.catalog_source not in [CatalogSource.GLUE]:
            raise ValueError(f"Invalid catalog source: {self.catalog_source}")
        if self.table_format not in [CatalogTableFormat.ICEBERG]:
            raise ValueError(f"Invalid table format: {self.table_format}")


class GlueCatalogIntegration(Resource):
    """
    CREATE [ OR REPLACE ] CATALOG INTEGRATION [IF NOT EXISTS]
      <name>
      CATALOG_SOURCE = { GLUE }
      TABLE_FORMAT = { ICEBERG }
      GLUE_AWS_ROLE_ARN = '<arn-for-AWS-role-to-assume>'
      GLUE_CATALOG_ID = '<glue-catalog-id>'
      [ GLUE_REGION = '<AWS-region-of-the-glue-catalog>' ]
      CATALOG_NAMESPACE = '<catalog-namespace>'
      ENABLED = { TRUE | FALSE }
      [ COMMENT = '{string_literal}' ]
    """

    resource_type = ResourceType.CATALOG_INTEGRATION
    props = Props(
        catalog_source=EnumProp("catalog_source", CatalogSource),
        table_format=EnumProp("table_format", CatalogTableFormat),
        glue_aws_role_arn=StringProp("glue_aws_role_arn"),
        glue_catalog_id=StringProp("glue_catalog_id"),
        catalog_namespace=StringProp("catalog_namespace"),
        glue_region=StringProp("glue_region"),
        enabled=BoolProp("enabled"),
        comment=StringProp("comment"),
    )
    scope = AccountScope()
    spec = _GlueCatalogIntegration

    def __init__(
        self,
        name: str,
        table_format: CatalogTableFormat,
        glue_aws_role_arn: str,
        glue_catalog_id: str,
        catalog_namespace: str,
        enabled: bool,
        glue_region: str = None,
        owner: str = "ACCOUNTADMIN",
        comment: str = None,
        **kwargs,
    ):
        kwargs.pop("catalog_source", None)
        super().__init__(**kwargs)
        self._data = _GlueCatalogIntegration(
            name=name,
            glue_aws_role_arn=glue_aws_role_arn,
            glue_catalog_id=glue_catalog_id,
            catalog_namespace=catalog_namespace,
            table_format=table_format,
            glue_region=glue_region,
            enabled=enabled,
            owner=owner,
            comment=comment,
        )


@dataclass(unsafe_hash=True)
class _ObjectStoreCatalogIntegration(ResourceSpec):
    name: ResourceName
    catalog_source: CatalogSource = CatalogSource.OBJECT_STORE
    table_format: CatalogTableFormat = CatalogTableFormat.ICEBERG
    enabled: bool = True
    comment: str = None

    def __post_init__(self):
        super().__post_init__()
        if self.catalog_source not in [CatalogSource.OBJECT_STORE]:
            raise ValueError(f"Invalid catalog source: {self.catalog_source}")
        if self.table_format not in [CatalogTableFormat.ICEBERG]:
            raise ValueError(f"Invalid table format: {self.table_format}")


class ObjectStoreCatalogIntegration(Resource):
    """
    CREATE [ OR REPLACE ] CATALOG INTEGRATION [IF NOT EXISTS]
        <name>
        CATALOG_SOURCE = { OBJECT_STORE }
        TABLE_FORMAT = { ICEBERG }
        ENABLED = { TRUE | FALSE }
        [ COMMENT = '{string_literal}' ]
    """

    resource_type = ResourceType.CATALOG_INTEGRATION
    props = Props(
        catalog_source=EnumProp("catalog_source", CatalogSource),
        table_format=EnumProp("table_format", CatalogTableFormat),
        enabled=BoolProp("enabled"),
        comment=StringProp("comment"),
    )
    scope = AccountScope()
    spec = _ObjectStoreCatalogIntegration

    def __init__(
        self,
        name: str,
        table_format: CatalogTableFormat,
        enabled: bool = True,
        comment: str = None,
        **kwargs,
    ):
        kwargs.pop("catalog_source", None)
        super().__init__(**kwargs)
        self._data = _ObjectStoreCatalogIntegration(
            name=name,
            table_format=table_format,
            enabled=enabled,
            comment=comment,
        )


CatalogIntegrationMap = {
    CatalogSource.GLUE: GlueCatalogIntegration,
    CatalogSource.OBJECT_STORE: ObjectStoreCatalogIntegration,
}


def _resolver(data: dict):
    return CatalogIntegrationMap[CatalogSource(data["catalog_source"])]


Resource.__resolvers__[ResourceType.CATALOG_INTEGRATION] = _resolver
