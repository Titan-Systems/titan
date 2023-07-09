# NOTES


## Secret system calls

CALL SYSTEM$ACCEPT_LEGAL_TERMS(?, ?)
SELECT SYSTEM$SHOW_IMPORTED_DATABASES()


## TODO

Write a python script in tools/gen.py that 
 - Takes in a URL
 - Passes the data to ChatGPT
 - Gives it the base class structure
 - Gives it 2-3 examples
 - Asks it to reason through the problem
 - Gives it the available props
 - Asks it to summarize the documentation page
 - Asks it to list what's necessary to create this class
    - create statement
    - new props
    - etc
 - Asks it to write the class
 - Prints the response 
 - Prints the class