-- Future Schema Privileges
GRANT CREATE ROW ACCESS POLICY ON FUTURE SCHEMAS IN DATABASE somedb TO ROLE somerole;
grant usage on future schemas in database mydb to role role1;

-- Future Schema Objects Privileges
GRANT INSERT ON FUTURE TABLES IN SCHEMA mydb.myschema TO ROLE somerole;