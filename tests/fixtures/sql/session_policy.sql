CREATE SESSION POLICY session_policy_prod_1
  SESSION_IDLE_TIMEOUT_MINS = 60
  SESSION_UI_IDLE_TIMEOUT_MINS = 30
  COMMENT = 'session policy for use in the prod_1 environment'
;