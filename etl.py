import os
import glob
import psycopg2
import pandas as pd
import numpy as np
from sql_queries import *


def process_song_file(cur, filepath):
    
    """Reads a song json file into a pandas dataframe and populates artists and songs tables
    
    Parameters
    ----------
    
    cur: cursor to the database
    
    filepath: path to the json file
    
    """
    
    
    # open song file
    df = pd.read_json(filepath,typ='series')

    song_data_all = list(df.values)
    song_data = [song_data_all[i] for i in [6,7,1,9,8]]
    
    # insert artist record
    artist_data = [song_data_all[i] for i in [1,5,4,2,3]]
    cur.execute(artist_table_insert, artist_data)
    
    # insert song record
    cur.execute(song_table_insert, song_data)
    
    


def process_log_file(cur, filepath):
    
    """Reads the user activity logs json file into pandas dataframe and populates users, time and songplays       table
    
    Parameters
    ----------
    
    cur: cursor to the database
    
    filepath: path to the json file
    
    """
    
    
    # open log file
    df = pd.read_json(filepath,lines=True)

    # filter by NextSong action
    df = df[df.page=='NextSong']

    # convert timestamp column to datetime
    datetime = pd.to_datetime(df.ts)
    # start_time = datetime.astype(np.int64)
    hour = datetime.dt.hour
    day = datetime.dt.day
    week = datetime.dt.isocalendar().week
    month = datetime.dt.month
    year = datetime.dt.year
    weekday = datetime.dt.weekday
    
    # insert time data records
    time_df =  pd.DataFrame({"start_time":datetime, 
                 "hour":hour, 
                 "day":day, 
                 "week":week, 
                 "month":month, 
                 "year":year, 
                 "weekday":weekday})

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

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
        songplay_data = (pd.to_datetime(row.ts),
                         row.userId,
                         row.level, 
                         songid, 
                         artistid, 
                         row.sessionId,
                         row.location,
                         row.userAgent )
        
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    
    """ Gets all the individual json files from the dataset and processes them to populate tables
    
    Parameters
    ----------
    
    cur : Cursor to the Database
    
    conn : Connection object of the Database
    
    filepath : path to the directory which contains all the json files
    
    func : one of {process_song_file or process_log_file}
       Function which processes the json files belonging to songs or the logs 
       
    """
    
    
    
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
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
    
    """Connects to database and calls processing functions
    """
    
    
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=postgres password=shubham")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()