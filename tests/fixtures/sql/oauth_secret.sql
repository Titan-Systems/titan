CREATE OR REPLACE SECRET my_oauth_secret
   TYPE = OAUTH2
   API_AUTHENTICATION = 'my_security_integration'
   OAUTH_SCOPES = ( 'scope1', 'scope2' )
   OAUTH_REFRESH_TOKEN = 'my_refresh_token'
   OAUTH_REFRESH_TOKEN_EXPIRY_TIME = '2022-12-31 23:59:59'
   COMMENT = 'OAuth2 secret for accessing external API';