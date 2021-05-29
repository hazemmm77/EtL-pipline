## Description
Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app.
the analytics team is particularly interested in understanding what songs users are listening to.
 I create a database schema and ETL pipeline for this analysis to be optimized for queries on song play analysis.
 Using the song and log datasets
 
 ## Database Design
 ### Fact Table
   **songplays** - records in log data associated with song plays i.e. records with page NextSong
   _songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent_

### Dimension Tables
  **users** - users in the app
  _user_id, first_name, last_name, gender, level_
  
  **songs** - songs in music database
   _song_id, title, artist_id, year, duration_
  
  **artists** - artists in music database
  _artist_id, name, location, latitude, longitude_
  
  **time** - timestamps of records in songplays broken down into specific units
  _start_time, hour, day, week, month, year, weekday_
  
  ## ETL Process
   build an ETL pipeline using Python . reading data from log files  from directories data\song_data and data\log_data and insert in  postgres tables which  i created previously.
   
  ## Project  files
 1. ```test.ipynb``` displays the first few rows of each table to let you check your database.
 2. ```create_tables.py``` drops and creates your tables. You run this file to reset your tables before each time you run your ETL scripts.
 3.  ```etl.ipynb``` reads and processes a single file from song_data and log_data and loads the data into your tables. This notebook contains detailed instructions on the ETL process for each of the tables.
 4. ```etl.py``` reads and processes files from song_data and log_data and loads them into your tables. You can fill this out based on your work in the ETL notebook.
 5. ```schema.py```  create database schema
 6. ```sparkifydb_erd``  database ERD
 7. ```sql_queries.py``` contains all your sql queries, and is imported into the last three files above.
 8. ```README.md``` provides discussion on your project.
 ## How To Run the Project
 ### Create Tables
  1. CREATE statements in sql_queries.py to create each table.
  2. DROP statements in sql_queries.py to drop each table if it exists.
  3.Run create_tables.py to create your database and tables.
  4.Run test.ipynb to confirm the creation of your tables with the correct columns. Make sure to click "Restart kernel" to close the connection to the database after running     this notebook.
### Build ETL Processes
  run  etl.ipynb to complete etl.py, where you'll process the entire datasets. Remember to run create_tables.py before running etl.py to reset your tables. Run test.ipynb to   confirm your records were successfully inserted into each table.
  
