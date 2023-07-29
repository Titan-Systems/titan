GRANT OPERATE ON WAREHOUSE report_wh TO ROLE analyst;
GRANT OPERATE ON WAREHOUSE report_wh TO ROLE analyst WITH GRANT OPTION;
GRANT SELECT ON ALL TABLES IN SCHEMA mydb.myschema to ROLE analyst;
GRANT ALL PRIVILEGES ON FUNCTION mydb.myschema.add5(number) TO ROLE analyst;

GRANT ALL PRIVILEGES ON FUNCTION mydb.myschema.add5(string) TO ROLE analyst;
GRANT USAGE ON PROCEDURE mydb.myschema.myprocedure(number) TO ROLE analyst;
GRANT CREATE MATERIALIZED VIEW ON SCHEMA mydb.myschema TO ROLE myrole;
GRANT SELECT,INSERT ON FUTURE TABLES IN SCHEMA mydb.myschema
TO ROLE role1;
grant usage on future schemas in database mydb to role role1;
