create or replace EXTERNAL FUNCTION local_echo(string_col VARCHAR, somesuch INTEGER)
    returns variant
    api_integration = demonstration_external_api_integration_01
    HEADERS = (
        'volume-measure' = 'liters',
        'distance-measure' = 'kilometers'
    )
    request_translator = "DB"."SCHEMA".function
    as 'https://xyz.execute-api.us-west-2.amazonaws.com/prod/remote_echo';

create or replace SECURE EXTERNAL FUNCTION local_echo2(string_col VARCHAR)
    returns variant
    api_integration = demonstration_external_api_integration_02
    as 'https://xyz.execute-api.us-west-2.amazonaws.com/prod/remote_echo';
