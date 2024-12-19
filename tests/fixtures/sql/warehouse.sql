
CREATE WAREHOUSE IF NOT EXISTS XSMALL_WH
    WITH
    WAREHOUSE_SIZE = 'XSMALL'
    WAREHOUSE_TYPE = 'STANDARD'
    AUTO_SUSPEND = 60
    AUTO_RESUME = FALSE
    initially_suspended = true
    RESOURCE_MONITOR = my_mon
    COMMENT = 'My XSMALL warehouse'
;



-- CREATE WAREHOUSE IF NOT EXISTS XSMALL_WH2 AUTO_SUSPEND = NULL;

CREATE WAREHOUSE lowercase_wh
warehouse_size = x6large
warehouse_type = snowpark-optimized
scaling_policy = economy
initially_suspended = true
;


