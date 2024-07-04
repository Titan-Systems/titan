CREATE STREAM mystream ON TABLE mytable;

CREATE STREAM mystream ON TABLE mytable AT(STREAM => 'oldstream');

CREATE OR REPLACE STREAM mystream ON TABLE mytable AT(STREAM => 'mystream');

CREATE STREAM mystream ON TABLE mytable BEFORE(STATEMENT => '8e5d0ca9-005e-44e6-b858-a8f5b37c5726');