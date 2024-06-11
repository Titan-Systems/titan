CREATE MATERIALIZED VIEW mymv
    COMMENT='Test view'
    AS
    SELECT col1, col2 FROM mytable;