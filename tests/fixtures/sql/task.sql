/*  TIMESTAMP_INPUT_FORMAT = 'YYYY-MM-DD HH24' -- This is a session parameter, not supported yet */

CREATE TASK t1
  SCHEDULE = '60 MINUTE'
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
AS
INSERT INTO mytable(ts) VALUES(CURRENT_TIMESTAMP);


CREATE TASK mytask_minute
  WAREHOUSE = mywh
  SCHEDULE = '5 MINUTE'
AS
INSERT INTO mytable(ts) VALUES(CURRENT_TIMESTAMP);

CREATE TASK mytask1
  WAREHOUSE = mywh
  SCHEDULE = '5 minute'
WHEN
  SYSTEM$STREAM_HAS_DATA('MYSTREAM')
AS
  INSERT INTO mytable1(id,name) SELECT id, name FROM mystream WHERE METADATA$ACTION = 'INSERT';

CREATE TASK task5
  AFTER task2, task3, task4
AS
INSERT INTO t1(ts) VALUES(CURRENT_TIMESTAMP);


CREATE TASK t1
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
  SCHEDULE = '2 minute'
  AS
  EXECUTE IMMEDIATE
  $$
  DECLARE
    radius_of_circle float;
    area_of_circle float;
  BEGIN
    radius_of_circle := 3;
    area_of_circle := pi() * radius_of_circle * radius_of_circle;
    return area_of_circle;
  END;
  $$;