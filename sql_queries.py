# DROP TABLES

songplay_table_drop = "DROP table IF EXISTS songplays"
user_table_drop = "DROP table IF EXISTS users"
song_table_drop = "DROP table IF EXISTS songs"
artist_table_drop = "DROP table IF EXISTS artists"
time_table_drop = "DROP table IF EXISTS time"

# CREATE TABLES

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (user_id int PRIMARY KEY, first_name varchar, last_name varchar, gender varchar, level varchar)
""")


artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (id SERIAL PRIMARY KEY, artist_id varchar UNIQUE, name varchar, location varchar, latitude real, longitude real)
""")


song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (song_id varchar PRIMARY KEY, title varchar, artist_id varchar, year int, duration float, CONSTRAINT fk_songs_artists FOREIGN KEY(artist_id) REFERENCES artists(artist_id))
""")


time_table_create = ("""CREATE TABLE IF NOT EXISTS time (start_time TIMESTAMP PRIMARY KEY, hour int, day int, week int, month int, year int, weekday varchar)
""")

songplay_table_create = (""" CREATE TABLE IF NOT EXISTS songplays (songplay_id SERIAL PRIMARY KEY, start_time TIMESTAMP, user_id int, level varchar, song_id varchar, artist_id varchar, session_id int, location varchar, user_agent varchar, CONSTRAINT fk_songplays_songs FOREIGN KEY (song_id) REFERENCES songs (song_id), CONSTRAINT fk_songplays_users FOREIGN KEY (user_id) REFERENCES users (user_id), CONSTRAINT fk_songplays_artists FOREIGN KEY (artist_id) REFERENCES artists (artist_id), CONSTRAINT fk_songplays_time FOREIGN KEY (start_time) REFERENCES time (start_time) )
""")


# INSERT RECORDS

songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) VALUES (%s, %s, %s, %s, %s,%s, %s, %s)
""")

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level) VALUES (%s, %s, %s, %s, %s) ON CONFLICT(user_id) DO UPDATE SET level = EXCLUDED.level
""")

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration) VALUES (%s, %s, %s, %s, %s)
""")

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude) VALUES (%s, %s, %s, %s, %s) ON CONFLICT(artist_id) DO NOTHING
""")


time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday) VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT(start_time) DO NOTHING
""")

# FIND SONGS

song_select = ("""SELECT s.song_id,a.artist_id FROM songs s INNER JOIN artists a ON s.artist_id = a.artist_id WHERE s.title = (%s) AND a.name = (%s) AND s.duration = (%s)""")

# QUERY LISTS

create_table_queries = [user_table_create, artist_table_create, song_table_create,  time_table_create,songplay_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]