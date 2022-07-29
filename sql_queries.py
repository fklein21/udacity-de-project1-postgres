# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

songplay_table_create = ("""
      CREATE TABLE IF NOT EXISTS 
            songplays (
                  songplay_id varchar PRIMARY KEY, 
                  start_time timestamp NOT NULL, 
                  user_id bigint NOT NULL, 
                  level varchar, 
                  song_id varchar, 
                  artist_id varchar, 
                  session_id varchar, 
                  location varchar, 
                  user_agent varchar);
""")

user_table_create = ("""
      CREATE TABLE IF NOT EXISTS 
            users (
                  user_id bigint PRIMARY KEY,
                  first_name varchar NOT NULL,
                  last_name varchar NOT NULL,
                  gender varchar,
                  level varchar);
""")

song_table_create = ("""
      CREATE TABLE IF NOT EXISTS 
            songs (
                  song_id varchar PRIMARY KEY, 
                  title varchar NOT NULL,
                  artist_id varchar NOT NULL,
                  year int,
                  duration double precision NOT NULL);
""")

artist_table_create = ("""
      CREATE TABLE IF NOT EXISTS 
            artists (
                  artist_id varchar PRIMARY KEY,
                  name varchar NOT NULL,
                  location varchar,
                  latitude double precision,
                  longitude double precision);
""")

time_table_create = ("""
      CREATE TABLE IF NOT EXISTS 
            time (
                  start_time timestamp PRIMARY KEY,
                  hour int,
                  day int,
                  week int,
                  month int,
                  year int,
                  weekday int);
""")

temp_log_data_create = ("""
      CREATE TEMP TABLE IF NOT EXISTS 
            temp_log_data (
                  songplay_id varchar PRIMARY KEY, 
                  start_time timestamp NOT NULL, 
                  user_id bigint NOT NULL, 
                  level varchar, 
                  song varchar, 
                  artist varchar, 
                  session_id varchar, 
                  location varchar, 
                  user_agent varchar,
                  length double precision);
""")

# INSERT RECORDS

songplay_table_insert = ("""
      INSERT INTO songplays (
            songplay_id, 
            start_time, 
            user_id, 
            level, 
            song_id, 
            artist_id, 
            session_id, 
            location, 
            user_agent) 
      VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
      ON CONFLICT (songplay_id)
      DO UPDATE SET 
            start_time = EXCLUDED.start_time, 
            user_id = EXCLUDED.user_id, 
            level = EXCLUDED.level, 
            song_id = EXCLUDED.song_id, 
            artist_id = EXCLUDED.artist_id, 
            session_id = EXCLUDED.session_id, 
            location = EXCLUDED.location, 
            user_agent = EXCLUDED.user_agent;

""")

user_table_insert = ("""
      INSERT INTO users (
            user_id,
            first_name, 
            last_name, 
            gender, 
            level)
      VALUES (%s, %s, %s, %s, %s)
      ON CONFLICT (user_id)
      DO UPDATE SET 
            first_name = EXCLUDED.first_name,
            last_name = EXCLUDED.last_name,
            gender = EXCLUDED.gender,
            level = EXCLUDED.level;
""")

user_table_batch_insert = ("""
      INSERT INTO users (
            user_id,
            first_name, 
            last_name, 
            gender, 
            level)
      VALUES %s
      ON CONFLICT (user_id)
      DO UPDATE SET 
            first_name = EXCLUDED.first_name,
            last_name = EXCLUDED.last_name,
            gender = EXCLUDED.gender,
            level = EXCLUDED.level;
""")

song_table_insert = ("""
      INSERT INTO songs (
            song_id,
            title,
            artist_id,
            year,
            duration)
      VALUES (%s, %s, %s, %s, %s)
      ON CONFLICT (song_id)
      DO UPDATE SET 
            title = EXCLUDED.title,
            artist_id = EXCLUDED.artist_id,
            year = EXCLUDED.year,
            duration = EXCLUDED.duration;
""")

song_table_batch_insert = ("""
      INSERT INTO songs (
            song_id,
            title,
            artist_id,
            year,
            duration)
      VALUES %s
      ON CONFLICT (song_id)
      DO UPDATE SET 
            title = EXCLUDED.title,
            artist_id = EXCLUDED.artist_id,
            year = EXCLUDED.year,
            duration = EXCLUDED.duration;
""")

artist_table_insert = ("""
      INSERT INTO artists (
            artist_id, 
            name, 
            location, 
            latitude, 
            longitude)
      VALUES (%s, %s, %s, %s, %s)
      ON CONFLICT (artist_id)
      DO UPDATE SET 
            name = EXCLUDED.name,
            location = EXCLUDED.location,
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude;
""")

artist_table_batch_insert = ("""
      INSERT INTO artists (
            artist_id, 
            name, 
            location, 
            latitude, 
            longitude)
      VALUES %s
      ON CONFLICT (artist_id)
      DO UPDATE SET 
            name = EXCLUDED.name,
            location = EXCLUDED.location,
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude;
""")


time_table_insert = ("""
      INSERT INTO time (
            start_time, 
            hour, 
            day, 
            week, 
            month, 
            year, 
            weekday)
      VALUES (%s, %s, %s, %s, %s, %s, %s)
      ON CONFLICT (start_time)
      DO NOTHING;
""")


time_table_batch_insert = ("""
      INSERT INTO time (
            start_time, 
            hour, 
            day, 
            week, 
            month, 
            year, 
            weekday)
      VALUES %s
      ON CONFLICT (start_time)
      DO NOTHING;
""")

sql_insert_temp_table = (""" 
      INSERT INTO temp_log_data (
          songplay_id, 
          start_time, 
          user_id, 
          level, 
          song, 
          artist, 
          session_id, 
          location, 
          user_agent,
          length) 
      VALUES %s
      ON CONFLICT (songplay_id)
      DO UPDATE SET 
          start_time = EXCLUDED.start_time, 
          user_id = EXCLUDED.user_id, 
          level = EXCLUDED.level, 
          song = EXCLUDED.song, 
          artist = EXCLUDED.artist, 
          session_id = EXCLUDED.session_id, 
          location = EXCLUDED.location, 
          user_agent = EXCLUDED.user_agent;
""")

# FIND SONGS

song_select = ("""
      SELECT song_id, s.artist_id
      FROM songs s
      JOIN artists a
         ON s.artist_id = a.artist_id
      WHERE title = %s and
            name = %s and
            duration = %s;
""")

# JOIN LOG_DATA WITH SONGS AND ARTISTS TABLES
join_log_data_songs_artists = ("""
      INSERT INTO songplays (
          songplay_id, 
          start_time, 
          user_id, 
          level, 
          song_id, 
          artist_id, 
          session_id, 
          location, 
          user_agent) 
      SELECT 
          tld.songplay_id, 
          tld.start_time, 
          tld.user_id, 
          tld.level, 
          s.song_id, 
          a.artist_id, 
          tld.session_id, 
          tld.location, 
          tld.user_agent
      FROM temp_log_data tld
      LEFT JOIN songs s ON
          tld.song = s.title and
          tld.length <= s.duration
      LEFT JOIN artists a ON
          tld.artist = a.name
      ON CONFLICT (songplay_id)
      DO UPDATE SET 
            start_time = EXCLUDED.start_time, 
            user_id = EXCLUDED.user_id, 
            level = EXCLUDED.level, 
            song_id = EXCLUDED.song_id, 
            artist_id = EXCLUDED.artist_id, 
            session_id = EXCLUDED.session_id, 
            location = EXCLUDED.location, 
            user_agent = EXCLUDED.user_agent;
""")


# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]