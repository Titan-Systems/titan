-- Global Privileges
GRANT IMPORT SHARE ON ACCOUNT TO ROLE somerole;
GRANT ALL PRIVILEGES ON ACCOUNT TO ROLE somerole;

-- Account Object Privileges
GRANT OPERATE ON WAREHOUSE report_wh TO ROLE analyst;
GRANT OPERATE ON WAREHOUSE report_wh TO ROLE analyst WITH GRANT OPTION;
GRANT ALL ON REPLICATION GROUP some_group TO ROLE somerole;

-- Schema Privileges
GRANT MODIFY ON SCHEMA someschema TO ROLE somerole;
GRANT ADD SEARCH OPTIMIZATION ON SCHEMA someschema TO ROLE somerole;
GRANT CREATE SNOWFLAKE.ML.FORECAST ON SCHEMA someschema TO ROLE somerole;
GRANT CREATE MATERIALIZED VIEW ON SCHEMA mydb.myschema TO ROLE myrole;


-- Schema Object Privileges
GRANT ALL PRIVILEGES ON FUNCTION mydb.myschema.add5(number) TO ROLE analyst;
GRANT ALL PRIVILEGES ON FUNCTION mydb.myschema.add5(string) TO ROLE analyst;
GRANT USAGE ON PROCEDURE mydb.myschema.myprocedure(number) TO ROLE analyst;


