import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """Insert song information into database.

    Reads a json file containing the song information,
    extracts the information about the song and the artist.
    The song and artist information are written the the songs and the artists tables,
    respectively.

    Args:
        cur (psycopg2.extensions.cursor): Cursor to the postgres database.
        filepath (str): Path of the json file containing the song information.
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    columns = ['song_id', 'title', 'artist_id', 'year', 'duration']
    song_data = list(df[columns].values[0])
    cur.execute(song_table_insert, song_data)

    # insert artist record
    artist_columns = ['artist_id', 'artist_name', 'artist_location',
                      'artist_latitude', 'artist_longitude']
    artist_data = list(df[artist_columns].values[0])
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """"Inserts the log information into the database.

    Reads the log infomation from a json file and extracts the information about the
    songplay events, the users, and the timestamps. The information are written to the
    songplays, users, time tables, respectively.

    Args:
        cur (psycopg2.extensions.cursor): Cursor to the postgres database.
        filepath (str): Path of the json file containing the log information.
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms', origin='unix')

    # insert time data records
    time_data = list(zip(t, t.dt.hour, t.dt.day, t.dt.isocalendar().week,
                     t.dt.month, t.dt.year, t.dt.weekday))
    column_labels = ['timestamp', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    time_df = pd.DataFrame(time_data, columns=column_labels)

    for _, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_columns = ['userId', 'firstName', 'lastName', 'gender', 'level']
    user_df = df[user_columns]

    # insert user records
    for _, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for _, row in df.iterrows():

        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        # If nothing was found, write None to the respective fields
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (f"{row.sessionId}-{row.ts}",
                            pd.to_datetime(row.ts, unit='ms', origin='unix'),
                            row.userId,
                            row.level,
                            songid,
                            artistid,
                            row.sessionId,
                            row.location,
                            row.userAgent,)
        cur.execute(songplay_table_insert, songplay_data)


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

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """Main function defining the connection to the database and call process_data."""
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    # Process the song and log data files.
    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
