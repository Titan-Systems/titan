CREATE NETWORK RULE corporate_network
  TYPE = AWSVPCEID
  VALUE_LIST = ('vpce-123abc3420c1931')
  MODE = INTERNAL_STAGE
  COMMENT = 'corporate privatelink endpoint';

CREATE NETWORK RULE cloud_network
  TYPE = IPV4
  VALUE_LIST = ('47.88.25.32/27')
  COMMENT ='cloud egress ip range';

CREATE NETWORK RULE external_access_rule
  TYPE = HOST_PORT
  MODE = EGRESS
  VALUE_LIST = ('example.com', 'company.com:443');