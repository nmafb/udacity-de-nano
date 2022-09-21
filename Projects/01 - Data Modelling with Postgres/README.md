# Udacity Data Engineering Nano Degree

## Project : Data Modelation - Sparkify
The purpose of this database is to get information about Saprkify songs and their users activity on their streaming app.
The main reason is to find out what songs are being listened by the users.

## How to Run Python scripts
With python installed, open a terminal with in the local folder of the file and execute:
* <code>python3 create_tables.py</code>
* <code>python3 etl.py</code>

## Files in the repository
* sql_queries.py   
    -> All the scripts needed to Create/Drop the tables, Insert statements and query

* create_tables.py 
    -> It Drops and creates the database Sparkify
    -> Executes the file sql_queries

* etl.ipynb
    -> Jupyter notebook with the full ETL process that transforms the data and fills all the tables
    
* etl.py
    -> python scripts with the full ETL process that transforms the data and fills all the tables.
    
* test.ipynb
    -> Jupyter notebook with the queries to the tables to validate that all the tables have the correct rows.
    
## Database Schema Design

The schema design is composed by 1 fact table (songplays) that holds the information about the usage of the songs from the Saprkyfi users.
This fact table is characterized by 4 dimension tables, that gives the detail of the Users, Artists, Songs and Time.

![image](/images/project_1_data_modelling_model.png)


* Songplays (Fact table)
    * Holds the information about the users songs playing.
    * Its composed by:
        * start_time timestamp, user_id, level, song_id, artist_id, session_id , location, user_agent
        
* Sons (Dimension table)
    * Holds the information about the songs played by the users
    * Its composed by:
        * song_id, title varchar, artist_id, year, duration
        
* Users (Dimension table)
    * Holds the information about the users that played the songs
    * Its composed by:
        * user_id, first_name, last_name, gender, level
        
* Artits (Domension table)
    * Holds the information about the artists from the songs played by the users
    * Its composed by:
        * artist_id, name, ocation, latitude, longitude
        
* Time (Dimenstion table)
    * Holds the information about the time from the songs played by the users
    * Its composed by:
        * start_time, hour, day, week, month, year, weekday


The ETL process is the main responsable for the extraction and transformation of the raw data from the system files, and load it into the tables defined in the star schema below.
* The file "song_data" contains all the information needed for the tables:
    * Songs
    * Artists
 
* The file "log_data", contains all the information needed for the tables
    * Users
    * Time
    * Songplays
    As it is the execution log of all the songs in the streaming app.