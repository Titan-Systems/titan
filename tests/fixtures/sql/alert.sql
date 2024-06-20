CREATE OR REPLACE ALERT myalert
  WAREHOUSE = mywarehouse
  SCHEDULE = '1 minute'
  IF( EXISTS(
    SELECT gauge_value FROM gauge WHERE gauge_value>200))
  THEN
    INSERT INTO gauge_value_exceeded_history VALUES (current_timestamp());

CREATE OR REPLACE ALERT alert_new_rows
  WAREHOUSE = my_warehouse
  SCHEDULE = '1 MINUTE'
  IF (EXISTS (
      SELECT *
      FROM my_table
      WHERE row_timestamp BETWEEN SNOWFLAKE.ALERT.LAST_SUCCESSFUL_SCHEDULED_TIME()
       AND SNOWFLAKE.ALERT.SCHEDULED_TIME()
  ))
  THEN CALL SYSTEM$SEND_EMAIL(...)
;

