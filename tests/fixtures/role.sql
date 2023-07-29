CREATE ROLE myrole;
CREATE OR REPLACE ROLE IF NOT EXISTS LOADER
    WITH TAG (purpose = 'pass butter')
    COMMENT = 'A role for loading data'
;