-- Valid UDF.  'N' must be capitalized.
CREATE OR REPLACE FUNCTION add5(n double)
  RETURNS double
  LANGUAGE JAVASCRIPT
  AS 'return N + 5;';

-- Valid UDF. Lowercase argument is double-quoted.
CREATE OR REPLACE FUNCTION add5_quoted("n" double)
  VOLATILE
  RETURNS double
  LANGUAGE JAVASCRIPT
  AS 'return n + 5;';

-- Invalid UDF. Error returned at runtime because JavaScript identifier 'n' cannot be resolved.
CREATE OR REPLACE FUNCTION add5_lowercase(n double)
  RETURNS double
  LANGUAGE JAVASCRIPT
  AS $$return n + 5;$$;

