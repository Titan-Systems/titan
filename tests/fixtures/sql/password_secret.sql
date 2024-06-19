

CREATE OR REPLACE SECRET my_password_secret
   TYPE = PASSWORD
   USERNAME = 'my_username'
   PASSWORD = 'my_password'
   COMMENT = 'Password secret for accessing external database';

