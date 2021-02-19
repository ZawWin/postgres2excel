# Self-serve python script to generate excel from database


## Project Background
This utility script was developed as part of the project that requires me to connect to serveral databases at once, collect the output from each database and produce an excel or csv extract from those results. The tool also has the proper error handling function implemented and will have an error log created in case of any errors during the run.

## The Setup
The script has 4 main components.
1. Collect the connection strings of the sites you are connecting
2.
  a. The user has an option to enter information in a config file.
  b. Alternatively, Gathering user input such as username, password, output folder location, output file during the run time.
4. Function to execute the intake of any sql file
5. Putting together the above 3 steps in a logical order and executing them

## How to execute
1. Copy the .py script to your local folder.
2. Open the terminal and cd into the folder where the file is stored.
3. Type in `python filename.py` 

## Execution Steps
  Once executed,
  1. It will ask you a series of yes or no questions.
  2. Depending on your answers, it will be executing your sql file across all sites or selected sites.
  3. It will display the completion message at the end of the run and output (excel or csv) will be ready at the folder that you specified.
  4. If there are any errors, error log will be created.
  
  
 
