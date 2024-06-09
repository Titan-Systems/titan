CREATE CATALOG INTEGRATION glueCatalogInt
  CATALOG_SOURCE=GLUE
  CATALOG_NAMESPACE='<catalog-namespace>'
  TABLE_FORMAT=ICEBERG
  GLUE_AWS_ROLE_ARN='<arn-for-aws-role-to-assume>'
  GLUE_CATALOG_ID='<catalog-id>'
  GLUE_REGION='<optional-aws-region-of-the-glue-catalog>'
  ENABLED=TRUE
  COMMENT='This is a test catalog integration';

CREATE CATALOG INTEGRATION myCatalogInt
  CATALOG_SOURCE=OBJECT_STORE
  TABLE_FORMAT=ICEBERG
  ENABLED=TRUE
  COMMENT='This is a test catalog integration';