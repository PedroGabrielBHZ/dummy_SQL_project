import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    I process a song file's information. Relevant song's data is inserted
    into songs data table and artist's data into artist's data table.

    :param cur: cursor variable
    :param filepath: the file path to the song file

    :returns: Nothing
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data_columns = ['song_id', 'title', 'artist_id', 'year', 'duration']
    song_data = list(df[song_data_columns].values[0])
    cur.execute(song_table_insert, song_data)

    # insert artist record
    artist_data_columns = ['artist_id', 'artist_name', 'artist_location',
                           'artist_latitude', 'artist_longitude']
    artist_data = list(df[artist_data_columns].values[0])
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    I process a log file's information. Timestamp data gets parsed into
    different granularities and used to populate time table. Relevant user
    data is inserted into users table and relevant songplay data gets inserted
    into songplays table.

    :param cur: cursor variable
    :param filepath: the file path to the log file

    :returns: Nothing
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df.loc[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = df['ts'].apply(pd.Timestamp, unit='ms')

    # insert time data records
    time_data = [t, t.dt.hour, t.dt.day, t.dt.weekofyear,
                 t.dt.month, t.dt.year, t.dt.dayofweek]
    column_labels = ['start_time', 'hour', 'day',
                     'week', 'month', 'year', 'weekday']
    time_df = pd.DataFrame(
        {column_labels[i]: time_data[i] for i in range(len(time_data))}
    )

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_data_columns = ['userId', 'firstName', 'lastName', 'gender', 'level']
    user_df = df[user_data_columns]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():

        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (pd.Timestamp(row.ts, unit='ms'),
                         row.userId, row.level, songid, artistid, row.location,
                         row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    I process all files in a directory using a specified callable object func.

    :param cur: cursor variable
    :param conn: connection variable
    :param filepath: the file path to the log file
    :param func: function used to parse a file

    :returns: Nothing
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
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
    """I execute program's main logic."""
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
