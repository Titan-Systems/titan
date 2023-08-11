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

/*CREATE VIEW employee_hierarchy (title, employee_ID, manager_ID, "MGR_EMP_ID (SHOULD BE SAME)", "MGR TITLE") AS (
   WITH RECURSIVE employee_hierarchy_cte (title, employee_ID, manager_ID, "MGR_EMP_ID (SHOULD BE SAME)", "MGR TITLE") AS (
      -- Start at the top of the hierarchy ...
      SELECT title, employee_ID, manager_ID, NULL AS "MGR_EMP_ID (SHOULD BE SAME)", 'President' AS "MGR TITLE"
        FROM employees
        WHERE title = 'President'
      UNION ALL
      -- ... and work our way down one level at a time.
      SELECT employees.title, 
             employees.employee_ID, 
             employees.manager_ID, 
             employee_hierarchy_cte.employee_id AS "MGR_EMP_ID (SHOULD BE SAME)", 
             employee_hierarchy_cte.title AS "MGR TITLE"
        FROM employees INNER JOIN employee_hierarchy_cte
       WHERE employee_hierarchy_cte.employee_ID = employees.manager_ID
   )
   SELECT * 
      FROM employee_hierarchy_cte
);*/

/*CREATE RECURSIVE VIEW employee_hierarchy_02 (title, employee_ID, manager_ID, "MGR_EMP_ID (SHOULD BE SAME)", "MGR TITLE") AS (
      -- Start at the top of the hierarchy ...
      SELECT title, employee_ID, manager_ID, NULL AS "MGR_EMP_ID (SHOULD BE SAME)", 'President' AS "MGR TITLE"
        FROM employees
        WHERE title = 'President'
      UNION ALL
      -- ... and work our way down one level at a time.
      SELECT employees.title, 
             employees.employee_ID, 
             employees.manager_ID, 
             employee_hierarchy_02.employee_id AS "MGR_EMP_ID (SHOULD BE SAME)", 
             employee_hierarchy_02.title AS "MGR TITLE"
        FROM employees INNER JOIN employee_hierarchy_02
        WHERE employee_hierarchy_02.employee_ID = employees.manager_ID
);*/