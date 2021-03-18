# Self-serve python script to generate excel or csv from multiple databases at once


## Project Background
This utility script was developed as part of the project that requires me to connect to serveral databases at once, collect the output from each database and produce an excel or csv extract from those results. The tool also has the proper error handling function implemented and will have an error log created in case of any errors during the run.


## How to execute
1. Copy the .py script to your local folder.
2. Open the terminal and cd into the folder where the file is stored.
3. Type in `python filename.py` 

## Execution Steps
  Once executed the python,
  1. The user will have an option to use a config file if he has one set up.
  2. If not, it will ask you a series of yes or no questions.
  3. Depending on your answers, it will be executing your sql file across all sites or selected sites.
  4. It will display the completion message at the end of the run and output (excel or csv) will be ready at the folder that you specified.
  5. If there are any errors, error log will be created.
  
  
 
