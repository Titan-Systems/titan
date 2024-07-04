
CREATE STORAGE INTEGRATION azure_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'AZURE'
  ENABLED = TRUE
  AZURE_TENANT_ID = '<tenant_id>'
  STORAGE_ALLOWED_LOCATIONS = ('azure://myaccount.blob.core.windows.net/mycontainer/path1/', 'azure://myaccount.blob.core.windows.net/mycontainer/path2/');


CREATE STORAGE INTEGRATION azure_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'AZURE'
  ENABLED = TRUE
  AZURE_TENANT_ID = 'a123b4c5-1234-123a-a12b-1a23b45678c9'
  STORAGE_ALLOWED_LOCATIONS = ('*')
  STORAGE_BLOCKED_LOCATIONS = ('azure://myaccount.blob.core.windows.net/mycontainer/path3/', 'azure://myaccount.blob.core.windows.net/mycontainer/path4/');

