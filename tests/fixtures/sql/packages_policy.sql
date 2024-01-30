CREATE OR REPLACE PACKAGES POLICY example_policy
LANGUAGE PYTHON
ALLOWLIST = ('numpy', 'pandas')
BLOCKLIST = ('os', 'sys')
ADDITIONAL_CREATION_BLOCKLIST = ('exec', 'eval')
COMMENT = 'This is an example packages policy.'
