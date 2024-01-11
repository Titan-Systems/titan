CREATE OR REPLACE DYNAMIC TABLE product
 TARGET_LAG = '20 minutes'
  WAREHOUSE = mywh
  REFRESH_MODE = AUTO
  INITIALIZE = ON_CREATE
  AS
    SELECT product_id, product_name FROM staging_table;


CREATE OR REPLACE DYNAMIC TABLE names
TARGET_LAG = DOWNSTREAM
WAREHOUSE = mywh
REFRESH_MODE = INCREMENTAL
INITIALIZE = ON_SCHEDULE
AS
SELECT var:id::int id, var:fname::string first_name,
var:lname::string last_name FROM raw;