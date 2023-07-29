CREATE OR REPLACE RESOURCE MONITOR my_mon_1
    WITH
        credit_quota=5
        FREQUENCY = DAILY
        START_TIMESTAMP = '2020-01-01 00:00:00'
        NOTIFY_USERS = ( teej, jack, jill )
;


CREATE OR REPLACE RESOURCE MONITOR my_mon_2
    WITH
        credit_quota=5
        FREQUENCY = DAILY
        START_TIMESTAMP = IMMEDIATELY
        NOTIFY_USERS = ( teej, jack, jill )
;