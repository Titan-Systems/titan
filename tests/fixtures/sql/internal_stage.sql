CREATE STAGE stage_with_encryption
  ENCRYPTION = (TYPE = 'SNOWFLAKE_FULL');

CREATE STAGE my_int_stage_1;

CREATE STAGE my_int_stage_2
  ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');

CREATE STAGE stage_with_directory
  DIRECTORY = (ENABLE = TRUE)
  COMMENT = 'This is a stage with a directory';