import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def insert_df_into_table(cur, table, df):
    """Inserting values in a dataframe into the database.

    Inserts values from a dataframe into a table. 
    The dataframe can only contain the columns to be inserted.

    Args:
        cur (psycopg2.extensions.cursor): Cursor to the postgres database.
        table (str): The name of the table to insert the data into.
        df (pandas.Dataframe): Dataframe with the data to insert into the table.
    """

    temp_dir = './temp_copy_to_sql/'
    os.makedirs(temp_dir, exist_ok=True)
    temp_file = temp_dir+'temp_copy_to_sql.csv'
    df.to_csv(temp_file, header=False, index=False, sep='|', quotechar="'", na_rep='NaN')
    with open(temp_file, 'r') as f_in:
        cur.copy_from(f_in, table, sep='|')

    os.remove(temp_file)
    if not os.listdir(temp_dir):
        os.rmdir(temp_dir)


def process_all_song_files(cur, files):
    """Inserts song information into database.

    Reads json files containing the song information,
    extracts the information about each song and each artist.
    The song and artist information are written the the songs and the artists tables,
    respectively.

    Args:
        cur (psycopg2.extensions.cursor): Cursor to the postgres database.
        files (list(str)): A list of strings with the paths of the json files 
                           containing the song information.
    """
    # open song files
    df = pd.concat((pd.read_json(file, lines=True) for file in files))

    # Make sure there are no duplicate songs
    df.drop_duplicates(subset='song_id', keep='last', inplace=True)

    # insert song records
    columns = ['song_id', 'title', 'artist_id', 'year', 'duration']
    insert_df_into_table(cur, 'songs', df[columns])

    # insert artist record
    artist_columns = ['artist_id', 'artist_name', 'artist_location',
                      'artist_latitude', 'artist_longitude']
    df_artist_data = df[artist_columns]
    df_artist_data = df_artist_data.drop_duplicates(subset='artist_id', keep='last')
    insert_df_into_table(cur, 'artists', df_artist_data)


def process_all_log_files(cur, files):
    """"Inserts the log information into the database.

    Reads the log information from a json file and extracts the information about the
    songplay events, the users, and the timestamps. The information are written to the
    songplays, users, time tables, respectively.

    Args:
        cur (psycopg2.extensions.cursor): Cursor to the postgres database.
        files (list(str)): A list of strings with the paths of the json files 
                           containing the log information.
    """
    # open log files
    df = pd.concat((pd.read_json(file, lines=True) for file in files), axis=0)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms', origin='unix')

    # insert time data records
    time_data = list(zip(t, t.dt.hour, t.dt.day, t.dt.week,
                     t.dt.month, t.dt.year, t.dt.weekday))
    column_labels = ['timestamp', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    df_time = pd.DataFrame(time_data, columns=column_labels).drop_duplicates()

    insert_df_into_table(cur, 'time', df_time)

    # load user table
    user_columns = ['userId', 'firstName', 'lastName', 'gender', 'level']
    df_user = df[user_columns]

    # quick checks for data quality
    df_user = df_user.dropna(axis=0)
    df_user = df_user[df_user['userId'] != '']
    df_user['userId'] = df_user['userId'].astype(int)
    df_user.drop_duplicates(subset='userId', keep='last', inplace=True)

    # insert user records
    insert_df_into_table(cur, 'users', df_user)

    # insert songplay records
    df['songplay_id'] = df['sessionId'].astype(str) + '-' + df['ts'].astype(str)
    df['start_time'] = pd.to_datetime(df.ts, unit='ms', origin='unix')
    songplay_columns = ['songplay_id', 'start_time', 'userId', 'level', 'song', 
                        'artist', 'sessionId', 'location', 'userAgent', 'length']
    df_songplay = df[songplay_columns]
    # insert into temporary table
    cur.execute(temp_log_data_create)
    insert_df_into_table(cur, 'temp_log_data', df_songplay)
    # join with data from songs and artists, and insert into songplays
    cur.execute(join_log_data_songs_artists)


def process_data(cur, conn, filepath, func):
    """Processes the log data and the song infos.

    The functions looks for all files with filename extension '.json' in filepath
    and its subdirectories.
    The files are then processed by calling the function func with cur and the
    respective filepaths as parameters.
    This way the content of the files (log data or song data) is written to the
    respective tables in the database.

    Args:
        cur (psycopg2.extensions.cursor): Cursor to the postgres database.
        conn (psycopg2.extensions.connection): Connection to the database.
        filepath (str): Path of a directory containing the json files in its sub directories.
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
    try:
        func(cur, all_files)
        conn.commit()
    except psycopg2.Error as e:
        print('Inserting data did not complete without errors.')
        print(e)
        print('Connection rollback.')
        conn.rollback()
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
