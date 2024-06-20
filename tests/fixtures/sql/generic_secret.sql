CREATE OR REPLACE SECRET my_generic_secret
   TYPE = GENERIC_STRING
   SECRET_STRING = 'my_generic_secret_string'
   COMMENT = 'Generic secret for various purposes';
