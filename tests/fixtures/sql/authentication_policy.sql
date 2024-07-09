CREATE AUTHENTICATION POLICY restrict_client_types_policy
  CLIENT_TYPES = ('SNOWFLAKE_UI')
  COMMENT = 'Auth policy that only allows access through the web interface';