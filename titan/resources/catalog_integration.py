from dataclasses import dataclass

from .resource import Resource, ResourceSpec, NamedResource
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


class GlueCatalogIntegration(NamedResource, Resource):
    """
    Description:
        Manages the integration of AWS Glue as a catalog in Snowflake, supporting the ICEBERG table format.

    Snowflake Docs:
        https://docs.snowflake.com/en/sql-reference/sql/create-catalog-integration

    Fields:
        name (string, required): The name of the catalog integration.
        table_format (string or CatalogTableFormat, required): The format of the table, defaults to ICEBERG.
        glue_aws_role_arn (string, required): The ARN for the AWS role to assume.
        glue_catalog_id (string, required): The Glue catalog ID.
        catalog_namespace (string, required): The namespace of the catalog.
        enabled (bool, required): Specifies whether the catalog integration is enabled.
        glue_region (string): The AWS region of the Glue catalog. Defaults to None.
        owner (string or Role): The owner role of the catalog integration. Defaults to "ACCOUNTADMIN".
        comment (string): An optional comment describing the catalog integration.

    Python:

        ```python
        glue_catalog_integration = GlueCatalogIntegration(
            name="some_catalog_integration",
            table_format="ICEBERG",
            glue_aws_role_arn="arn:aws:iam::123456789012:role/SnowflakeAccess",
            glue_catalog_id="some_glue_catalog_id",
            catalog_namespace="some_namespace",
            enabled=True,
            glue_region="us-west-2",
            comment="Integration for AWS Glue with Snowflake."
        )
        ```

    Yaml:

        ```yaml
        catalog_integrations:
          - name: some_catalog_integration
            table_format: ICEBERG
            glue_aws_role_arn: arn:aws:iam::123456789012:role/SnowflakeAccess
            glue_catalog_id: some_glue_catalog_id
            catalog_namespace: some_namespace
            enabled: true
            glue_region: us-west-2
            comment: Integration for AWS Glue with Snowflake.
        ```
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
        super().__init__(name, **kwargs)
        self._data: _GlueCatalogIntegration = _GlueCatalogIntegration(
            name=self._name,
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
    owner: Role = "ACCOUNTADMIN"

    def __post_init__(self):
        super().__post_init__()
        if self.catalog_source not in [CatalogSource.OBJECT_STORE]:
            raise ValueError(f"Invalid catalog source: {self.catalog_source}")
        if self.table_format not in [CatalogTableFormat.ICEBERG]:
            raise ValueError(f"Invalid table format: {self.table_format}")


class ObjectStoreCatalogIntegration(NamedResource, Resource):
    """
    Description:
        Manages the integration of an object store as a catalog in Snowflake, supporting the ICEBERG table format.

    Snowflake Docs:
        https://docs.snowflake.com/en/sql-reference/sql/create-catalog-integration

    Fields:
        name (string, required): The name of the catalog integration.
        table_format (string or CatalogTableFormat, required): The format of the table, defaults to ICEBERG.
        enabled (bool): Specifies whether the catalog integration is enabled. Defaults to True.
        comment (string): An optional comment describing the catalog integration.

    Python:

        ```python
        object_store_catalog_integration = ObjectStoreCatalogIntegration(
            name="some_catalog_integration",
            table_format="ICEBERG",
            enabled=True,
            comment="Integration for object storage."
        )
        ```

    Yaml:

        ```yaml
        catalog_integrations:
          - name: some_catalog_integration
            table_format: ICEBERG
            enabled: true
            comment: Integration for object storage.
        ```
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
        owner: str = "ACCOUNTADMIN",
        **kwargs,
    ):
        kwargs.pop("catalog_source", None)
        super().__init__(name, **kwargs)
        self._data: _ObjectStoreCatalogIntegration = _ObjectStoreCatalogIntegration(
            name=self._name,
            table_format=table_format,
            enabled=enabled,
            comment=comment,
            owner=owner,
        )


CatalogIntegrationMap = {
    CatalogSource.GLUE: GlueCatalogIntegration,
    CatalogSource.OBJECT_STORE: ObjectStoreCatalogIntegration,
}


def _resolver(data: dict):
    return CatalogIntegrationMap[CatalogSource(data["catalog_source"])]


Resource.__resolvers__[ResourceType.CATALOG_INTEGRATION] = _resolver
