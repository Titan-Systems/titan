create or replace function clean_table(table_name STRING)
  returns int
  language python
  runtime_version = '3.8'
  packages = ('snowflake-snowpark-python')
  handler = 'main'
AS
$$
def main(_): return 42
$$;
