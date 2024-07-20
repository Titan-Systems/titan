CREATE NOTEBOOK mynotebook;

CREATE NOTEBOOK mynotebook
 QUERY_WAREHOUSE = my_warehouse;

CREATE NOTEBOOK mynotebook
 FROM '@my_db.my_schema.my_stage'
 MAIN_FILE = 'my_notebook_file.ipynb'
 QUERY_WAREHOUSE = my_warehouse;