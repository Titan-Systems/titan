CREATE STREAM mystream ON TABLE mytable;

CREATE STREAM mystream ON VIEW myview;

CREATE STREAM my_ext_table_stream ON EXTERNAL TABLE my_ext_table INSERT_ONLY = TRUE;

CREATE STREAM dirtable_mystage_s ON STAGE mystage;

/* CREATE STREAM mystream ON TABLE mytable BEFORE (TIMESTAMP => TO_TIMESTAMP(40*365*86400));*/

/* CREATE STREAM mystream ON TABLE mytable AT (TIMESTAMP => TO_TIMESTAMP_TZ('02/02/2019 01:02:03', 'mm/dd/yyyy hh24:mi:ss'));*/

/* CREATE STREAM mystream ON TABLE mytable AT(OFFSET => -60*5);*/

CREATE STREAM mystream ON TABLE mytable AT(STREAM => 'oldstream');

CREATE OR REPLACE STREAM mystream ON TABLE mytable AT(STREAM => 'mystream');

CREATE STREAM mystream ON TABLE mytable BEFORE(STATEMENT => '8e5d0ca9-005e-44e6-b858-a8f5b37c5726');