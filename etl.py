import os
import glob
import psycopg2
import psycopg2.extras
import pandas as pd
from sql_queries import *


def insert_df_into_table(cur, sql_query, df):
    """Inserting values in a dataframe into the database.

    Inserts values from a dataframe into a table. 
    The dataframe can only contain the columns to be inserted.

    Args:
        cur (psycopg2.extensions.cursor): Cursor to the postgres database.
        sql_query (str): String with the sql query.
        df (pandas.Dataframe): Dataframe with the data to insert into the table.
    
    Example:
        Inserting song data into a songs table with the columns
        (song_id, title, artist_id, year, duration)
        sql_query = "INSERT INTO songs (song_id, title, artist_id, year, duration)\
                     VALUES %s\
                     ON CONFLICT (song_id)\
                     DO UPDATE SET title = EXCLUDED.title,\
                                   artist_id = EXCLUDED.artist_id,\
                                   year = EXCLUDED.year,\
                                   duration = EXCLUDED.duration;\
        df = pandas.Dataframe with the same columns and same data types.
    """
    data = [tuple(ii) for ii in df.to_numpy()]

    try:
        psycopg2.extras.execute_values(cur, sql_query, data)
    except psycopg2.Error as e:
        print("Error while inserting pandas.Dataframe into the table:")
        print(e)


def process_all_song_files(cur, files):
    """Insert song information into database.

    Reads a json file containing the song information,
    extracts the information about the song and the artist.
    The song and artist information are written the the songs and the artists tables,
    respectively.

    Args:
        cur (psycopg2.extensions.cursor): Cursor to the postgres database.
        filepath (str): Path of the json file containing the song information.
    """
    # open song files
    df = pd.concat((pd.read_json(file, lines=True) for file in files))

    # Make sure there are no duplicate songs
    df.drop_duplicates(subset='song_id', keep='last', inplace=True)

    # insert song records
    columns = ['song_id', 'title', 'artist_id', 'year', 'duration']
    insert_df_into_table(cur, song_table_batch_insert, df[columns])

    # insert artist record
    artist_columns = ['artist_id', 'artist_name', 'artist_location',
                      'artist_latitude', 'artist_longitude']
    df_artist_data = df[artist_columns]
    df_artist_data = df_artist_data.drop_duplicates(subset='artist_id', keep='last')
    insert_df_into_table(cur, artist_table_batch_insert, df_artist_data)


def process_all_log_files(cur, files):
    """"Inserts the log information into the database.

    Reads the log infomation from a json file and extracts the information about the
    songplay events, the users, and the timestamps. The information are written to the
    songplays, users, time tables, respectively.

    Args:
        cur (psycopg2.extensions.cursor): Cursor to the postgres database.
        filepath (str): Path of the json file containing the log information.
    """
    # open log files
    df = pd.concat((pd.read_json(file, lines=True) for file in files), axis=0)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms', origin='unix')

    # insert time data records
    time_data = list(zip(t, t.dt.hour, t.dt.day, t.dt.isocalendar().week,
                     t.dt.month, t.dt.year, t.dt.weekday))
    column_labels = ['timestamp', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    df_time = pd.DataFrame(time_data, columns=column_labels).drop_duplicates()

    insert_df_into_table(cur, time_table_batch_insert, df_time)

    # load user table
    user_columns = ['userId', 'firstName', 'lastName', 'gender', 'level']
    df_user = df[user_columns]

    # quick checks for data quality
    df_user = df_user.dropna(axis=0)
    df_user = df_user[df_user['userId'] != '']
    df_user['userId'] = df_user['userId'].astype(int)
    df_user.drop_duplicates(subset='userId', keep='last', inplace=True)

    # insert user records
    insert_df_into_table(cur, user_table_batch_insert, df_user)

    # insert songplay records
    df['songplay_id'] = df['sessionId'].astype(str) + '-' + df['ts'].astype(str)
    df['start_time'] = pd.to_datetime(df.ts, unit='ms', origin='unix')
    songplay_columns = ['songplay_id', 'start_time', 'userId', 'level', 'song', 
                        'artist', 'sessionId', 'location', 'userAgent', 'length']
    df_songplay = df[songplay_columns]
    # insert into temporary table
    cur.execute(temp_log_data_create)
    insert_df_into_table(cur, sql_insert_temp_table, df_songplay)
    cur.execute(join_log_data_songs_artists)


def process_data(cur, conn, filepath, func):
    """Processes the log data and the song infos.

    The functions looks for all files with filename extension '.json' in filepath
    and its subdirectories.
    Each file is processed separately by calling the function func with cur and the
    respective filepath as parameters.
    This way the content of the files (log data or song data) is written to the
    respective tables in the database.

    Args:
        cur (psycopg2.extensions.cursor): Cursor to the postgres database.
        conn (psycopg2.extensions.connection): Connection to the database.
        filepath (str): Path containing the json files in its sub directories.
        func (function): Function used to process the files.
    """
    # get all files matching extension from directory
    all_files = []
    for root, _, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))


    # process files with the appropriate function
    func(cur, all_files)
    conn.commit()
    print(f'{num_files} files processed.')


def main():
    """Main function defining the connection to the database and call process_data."""
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    # Process the song and log data files.
    process_data(cur, conn, filepath='data/song_data', func=process_all_song_files)
    process_data(cur, conn, filepath='data/log_data', func=process_all_log_files)

    conn.close()


if __name__ == "__main__":
    main()
