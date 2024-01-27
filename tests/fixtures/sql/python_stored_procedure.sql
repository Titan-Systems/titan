create or replace procedure clean_table(table_name STRING)
  returns int
  language python
  runtime_version = '3.8'
  packages = ('snowflake-snowpark-python')
  handler = 'clean_table_handler'
AS
$$
import snowflake.snowpark

def clean_table_handler(session: snowflake.snowpark.session.Session,
                        table_name: str) -> int:
  table = session.table(table_name)
  result = table.delete(~table['fruit'].rlike('[a-z]+'))
  # equivalent to `DELETE FROM dirty_data WHERE fruit NOT RLIKE '[a-z]+';`
  
  return result.rows_deleted
$$;
