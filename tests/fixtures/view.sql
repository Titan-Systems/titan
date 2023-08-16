CREATE VIEW myview
  COMMENT='Test view'
AS
  SELECT col1, col2 FROM mytable;

CREATE OR REPLACE SECURE VIEW myview
  COMMENT='Test secure view'
AS
  SELECT col1, col2 FROM mytable;

CREATE VIEW employee_hierarchy
  (title COMMENT 'employee title', employee_ID, manager_ID, "MGR_EMP_ID (SHOULD BE SAME)", "MGR TITLE")
AS
  SELECT * FROM employees;

