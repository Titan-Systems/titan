# How to install Titan

Snowflake app (preferred)

```SQL
-- AWS
CREATE STAGE titan_aws URL = 's3://titan-snowflake/';
EXECUTE IMMEDIATE FROM @titan_aws/install;

-- Google Cloud
CREATE STORAGE INTEGRATION titan_storage_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'GCS'
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('gcs://titan-snowflake/')
;

CREATE STAGE titan_gcp
    URL = 'gcs://titan-snowflake/'
    STORAGE_INTEGRATION = titan_storage_int;

EXECUTE IMMEDIATE
    FROM @titan_gcp/install;
```


Python package via pip

```sh
python -m pip install git+https://github.com/teej/titan.git
```