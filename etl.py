import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """ read data from  log files(song files) and insert in
    to database(songs table, artists table """
    # open song file
    df =pd.read_json(filepath,lines=True)

    # insert song record
    song_data =df[['song_id','title','artist_id',
                   'year','duration']].values[0].tolist()
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data =df[['artist_id','artist_name','artist_location',
                     'artist_latitude','artist_longitude']].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """ read data from  log files(log files) and insert in to
     database (time table, users table) """
    # open log file
    df =pd.read_json(filepath,lines=True)

    # filter by NextSong action
    df =df[df['page']=='NextSong']

    # convert timestamp column to datetime
    df['ts'] = pd.to_datetime(df['ts'], unit='ms')
    df['start_time'] = df['ts'].dt.time
    df['hour'] = df['ts'].dt.hour
    df['day'] = df['ts'].dt.day
    df['week'] = df['ts'].dt.week
    df['month'] = df['ts'].dt.month
    df['year'] = df['ts'].dt.year
    df['weekday'] = df['ts'].dt.weekday
    t = df[['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']]

    
    # insert time data records
    time_data =  (t.start_time,t.hour, t.day, t.week, t.month, t.year,t.weekday)
    column_labels = t.columns.tolist()
    time_df =pd.DataFrame.from_dict(dict(zip(column_labels, time_data)),orient='columns')

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId','firstName','lastName','gender','level']]

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
        songplay_data =(row.start_time,row.userId,row.level,songid,artistid,
                        row.sessionId,row.userAgent,row.location)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """ get all files from directory which contains log files
    and make processing opration ( read from it and insert in database """
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
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb "
                            "user=postgres password=Admin port=5435")
    cur = conn.cursor()

    process_data(cur, conn, filepath='C:\data\song_data', func=process_song_file)
    process_data(cur, conn, filepath='C:\data\log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()