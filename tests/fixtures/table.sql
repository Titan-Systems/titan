CREATE TABLE mytable (id INT PRIMARY KEY, amount NUMBER);
CREATE TABLE example (col1 number comment 'a column comment') COMMENT='a table comment';

-- Name includes schema
CREATE TABLE someschema.sometable (id int);


-- Name includes db and schema
-- CREATE TABLE somedb.someschema.sometable (id int);

CREATE TABLE someschema.sometable (modified TIMESTAMP_NTZ(9) NOT NULL);