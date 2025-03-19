# Sparkify Data Warehouse
This data warehouse is designed for Sparkify a virtual music streaming startup who wants to analyze its user activity and song preferences.  This data warehouse enables analytical queries on song play events and user behaviors, supporting business decisions such as identifying the most popular songs and artists, understanding peak listening times, analyzing user retention and engagement.

# Tools
 Amazon Redshift, S3


## Schema design
The data warehouse follows a star schema design consisting of 1 fact table and 4 dimension tables

**Fact Table**
- songplays : records of song plays, linking users and songs with event timestamps
    > columns : songplay_id, start_time, user_id, level
    - Distribution strategy: Key
    - Distribution key : user_id
    - Sort Key : start_time
The choice of user_id as distribution key ensures all records for a given user are stored in the same node minimizing data shuffling when joining with the users table, and start_time is used as sort key to enable efficeint time-based analysis. 

**Dimension Tables**
- users : user details in the app such as names, gender and subscription level.
    > columns : user_id, first_name, last_name, gender, level, song_id, artist_id, session_id, location, user_agent

- songs : song details such title, artists, year of release and duration.
    > columns : song_id, title, artist_id, year, duration

- artists : artist details such as name, location, latitude, longitude.
    > columns : artist_id, name, location, latitude, longitude

- time : timestamps of records in songplays extracted into hour, day, week, month and year.
    > columns : start_time, hour, day, week, month, year, weekday

    - Distribution strategy : ALL
Distribution starategy of All  ensures the dimension tables are fully replicated across all nodes to minimize data shuffling and improve query performance.

## ETL Pipeline
The ETL pipeline extracts raw JSON data from S3, stages it in Redshift before transforming into the star schema for analysis. The process follows these steps :
- 1. Extract : Copy raw log and song data from S3 into staging tables
- 2. Transform : Clean and process data to match the model 
- 3. Load : Insert transformed data into the star schema tables


## Scripts

### sql_queries.py

This script contains all the **DROP**, **CREATE** and **INSERT** queries related to the ETL process. This script will be used from the `etl.py` and `create_tables.py`.


### create_tables.py

This script will drop and create the tables schema  defined in `sql_queries.py`. Run the code below in the command shell after updating the configuration file :

```
python create_tabels.py
```

### etl.py

This script implements the ETL process. The script will copy JSON data from S3 bucket defined in configuration file into redshift staging tables and insert into the star schema tables defined in `create_tables.py`. Run the code below in the command shell after successful run of `create_tables.py .

```
python etl.py
```
## Test Queries
1. Busiest time of the day
```
SELECT t.hour AS time_of_day, sum(ss.duration)/3600 AS hour_spent FROM songplays sp
JOIN time t on (sp.start_time = t.start_time) JOIN songs ss on (ss.song_id = sp.song_id)
GROUP BY time_of_day ORDER BY hour_spent DESC;
```

2. Most played song 
```
SELECT s.title AS Song_title, a.name as Artist_name, count(sp.songplay_id) as frequency
FROM songplays sp JOIN songs s ON (s.song_id = sp.song_id) JOIN artists a ON (a.artist_id = sp.artist_id)
GROUP BY song_title, artist_name ORDER BY frequency DESC LIMIT 10;

```