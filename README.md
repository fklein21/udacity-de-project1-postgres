# Project 1: Data Modeling with Postgres

<p style='text-align: right;'> 29/7/22 </p>

### Udacity Bosch AI Talent Accelerator Scholarship
### Ferdinand Kleinschroth
(ferdinand.kleinschroth@gmx.de)

## Description of the project

The objective of the project was to create a database for the song library and the user log data of the music streaming service Sparkify. This database will then serve as a data provider for the analytics team.

The data consists of:
- a song library in the form of a collection of json files with information about each song and
- log data about user activity, also in the form of json files.

A PostgreSQL database sparkifydb was created containing the following tables:
- songplays (fact table): Contains all entries of the log files which are associated with a song play. The columns are   
  - `songplay_id`, 
  - `start_time`, 
  - `user_id`, 
  - `level`, 
  - `song_id`, 
  - `artist_id`, 
  - `session_id`, 
  - `location`, 
  - `user_agent`
- users (dimension table): Contains user information.
  - `user_id`, 
  - `first_name`, 
  - `last_name`, 
  - `gender`, 
  - `level`
- songs (dimension table): Contains information about the songs in the song library.
  - `song_id`, 
  - `title`, 
  - `artist_id`, 
  - `year`, 
  - `duration`
- artists (dimension table): Contains information about the artists.
  - `artist_id`, 
  - `name`, 
  - `location`, 
  - `latitude`, 
  - `longitude`
- time (dimension table): Timestamps of each log event split up into hour, day, ...
  - `start_time`, 
  - `hour`, 
  - `day`, 
  - `week`, 
  - `month`, 
  - `year`, 
  - `weekday`

This database enables the analytics team at Sparkify to efficiently analyze the data.


## How to run the scripts

Create database, drop existing tables and create the necessary tables:
```bash
python create_tables.py
```

Insert the information into the database. (This script assumes the log and song files are in the folders `data/log_data` and `data/song_data`, respectively.)
```bash
python etl.py
```

The following packages are necessary to run the scripts: `pandas`, `psycopg2-binary`

## Files in the repository

- `create_tables.py` : Python script that creates the database, drops existing tables and creates the necessary tables.
- `docker-compose.yaml` : Docker compose file to run a database server locally.
- `etl.ipynb`: Jupyter notebook to develop the pipeline before putting the logic in the Python script.
- `etl.py` : inserts the information by copying the data to a temporary csv file and reading it into database from this file.
- `README.md` : This readme file
- `requirements_dev.txt` : Required packages to develop the pipeline.
- `sql_queries.py` : Contains the sql queries used by the pipeline.
- `test.ipynb` : Notebook with sanity checks of the created database.
- `test_queries.ipynb` : Example queries.

## Discussoin of the database schema design and ETL pipeline

The Star schema with the table for each songplay event at the center and songs, artists, users and time dimension tables allows for simple queries by the analytics team. 

The time table allows for easy partitions by different time intervals as e. g. hour of the day or day of the week.

The file `etl.py` makes use of the PostgreSQL COPY_FROM function since this is faster than inserting the rows one by one.

## Example queries

Some example queries can be found in `test_queries.ipynb`.

Who are the most active users?
```sql
SELECT 
    user_id,
    first_name || ' ' || last_name AS full_name,
    gender,
    location,
    count(user_id)
FROM songplays
NATURAL JOIN users
GROUP BY 1, 2, 3, 4
ORDER BY 5 DESC
LIMIT 10;
```
| user_id   | full_name         | gender    | location                                  | count |
| --------- | ----------------- | --------- | ----------------------------------------- | ----- |
| 97        | Kate Harrell      | F         | Lansing-East Lansing, MI                  | 557   |
| 15        | Lily Koch         | F         | Chicago-Naperville-Elgin, IL-IN-WI        | 462   |
| 44        | Aleena Kirby      | F         | Waterloo-Cedar Falls, IA                  | 397   |
| 24        | Layla Griffin     | F         | Lake Havasu City-Kingman, AZ              | 321   |
| 73        | Jacob Klein       | M         | Tampa-St. Petersburg-Clearwater, FL       | 289   |
| 36        | Matthew Jones     | M         | Janesville-Beloit, WI                     | 241   |
| 88        | Mohammad Rodriguez| M         | Sacramento--Roseville--Arden-Arcade, CA   | 241   |
| 95        | Sara Johnson      | F         | Winston-Salem, NC                         | 213   |
| 16        | Rylan George      | M         | Birmingham-Hoover, AL                     | 208   |
| 85        | Kinsley Young     | F         | Red Bluff, CA                             | 178   |

<br>

At which day of the week are most users accessing the service?
```sql
SELECT 
    weekday,
    COUNT(user_id)
FROM songplays
NATURAL JOIN time
GROUP BY 1
ORDER BY 2 DESC;
```
| weekday   | count |
| --------- | ----- |
| 2	        | 1364  |
| 4	        | 1295  |
| 1	        | 1071  |
| 3	        | 1052  |
| 0	        | 1014  |
| 5	        | 628   |
| 6	        | 396   |

