import os
import glob
import psycopg2
import numpy as np
import pandas as pd
from sql_queries import *
from psycopg2.extensions import register_adapter, AsIs
from io import StringIO


def cd(cur, df, table_name, sep=',', null=False):
    """
    This procedure accepts a dataframe that needs to be copied to a 
    postgres table.
    It first saves the dataframe as a csv to the buffer and then the 
    table is copied to the 
    postgres table. 
    
    INPUTS: 
    * cur the cursor variable
    * df is the dataframe that needs to be converted to a csv 
    * table_name - table into which the csv needs to be copied 
    * sep = comma separated
    """
    sio = StringIO()
    sio.write(df.to_csv(sep=sep, index=None, header=None))
    # Write the Pandas DataFrame as a csv to the buffer
    sio.seek(0)
    if null:
        cur.copy_from(sio, table_name, columns=df.columns, sep=sep, null="")
    else:
        cur.copy_from(sio, table_name, columns=df.columns, sep=sep)


def process_song_file(cur, filepath):
    """
    This procedure processes a song file whose filepath has been provided 
    as an arugment.
    It extracts the song information in order to store it into the songs 
    table.
    Then it extracts the artist information in order to store it into the 
    artists table.

    INPUTS: 
    * cur the cursor variable
    * filepath the file path to the song file
    """
    # open song file
    df = pd.read_json(filepath, lines=True)
    # insert song record
    song_data = list(
        df.loc[0, ["song_id", "title", "artist_id", "year", 
                   
                   "duration"]].values
    )
    cur.execute(song_table_insert, song_data)

    # insert artist record
    artist_data = list(
        df.loc[
            0,
            [
                "artist_id",
                "artist_name",
                "artist_location",
                "artist_latitude",
                "artist_longitude",
            ],
        ].values
    )
    artist_data = [
        None if isinstance(el, np.float64) and np.isnan(el) else el
        for el in artist_data
    ]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    This procedure processes a log file whose filepath has been provided as an arugment.
    It extracts and transforms the log information in order to store it into the time, user 
    and songplay tables.
    

    INPUTS: 
    * cur the cursor variable
    * filepath the file path to the song file
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df["page"] == "NextSong"]

    # convert timestamp column to datetime
    ts = df["ts"]
    tsdt = pd.to_datetime(df["ts"],unit='ms')

    # insert time data records
    time_data = (tsdt.values, tsdt.dt.hour, tsdt.dt.day, tsdt.dt.week,
                 tsdt.dt.month, tsdt.dt.year, tsdt.dt.weekday)
    column_labels = ("start_time", "hour", "day",
                     "week", "month", "year", "weekday")
    combined_dict = dict(zip(column_labels, time_data))
    time_df = pd.DataFrame(combined_dict)
    
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    

    

    # load user table
    user_df = df[["userId", "firstName", "lastName", "gender", "level"]]
    user_df_columns = {"userId": "user_id", "firstName": "first_name",
                       "lastName": "last_name"}
    
    user_df = user_df.rename(columns=user_df_columns)
  
    
    uder_df = user_df.drop_duplicates(subset='user_id')
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

#     cd(cur, user_df, "users")



    # insert songplay records
    for index, row in df.iterrows():
        # get song_id and artist_id from song and artist tables
        cur.execute(song_select, (row.song, row.artist, round(row.length)))
        result = cur.fetchone()
        (song_id, artist_id) = (result if result else (None, None))

        if song_id is None or artist_id is None:
            continue

        # insert songplay record
        songplay_data = (round(row.ts / 1000.0), row.userId, row.level, song_id, \
                         artist_id, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    This procedure extracts json files from their respective directory and passes
    them to process_song_file and process_log_file functions for further 
    processing.
    

    INPUTS: 
    * cur the cursor variable
    * filepath the file path to the song file
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, "*.json"))
        for f in files:
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print("{} files found in {}".format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print("{}/{} files processed.".format(i, num_files))


def main():
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=sparkifydb user=student password=student"
    )
    cur = conn.cursor()

    process_data(cur, conn, filepath="data/song_data", func=process_song_file)
    process_data(cur, conn, filepath="data/log_data", func=process_log_file)

    conn.close()


if __name__ == "__main__":
    register_adapter(np.int64, AsIs)
    register_adapter(np.float64, AsIs)
    main()


