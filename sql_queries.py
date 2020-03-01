# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays CASCADE;"
user_table_drop = "DROP TABLE IF EXISTS users CASCADE;"
song_table_drop = "DROP TABLE IF EXISTS songs CASCADE;"
artist_table_drop = "DROP TABLE IF EXISTS artists CASCADE;"
time_table_drop = "DROP TABLE IF EXISTS time CASCADE;"

# CREATE TABLES

songplay_table_create = (""" CREATE TABLE IF NOT EXISTS songplays (
            songplay_id serial PRIMARY KEY, 
            start_time bigint NOT NULL,
            user_id varchar NOT NULL, 
            level varchar(255), 
            song_id varchar(255), 
            artist_id varchar(255), 
            session_id int, 
            location varchar(255), 
            user_agent varchar(255)
            );""")

user_table_create = ("""
            CREATE TABLE IF NOT EXISTS users (
            user_id varchar PRIMARY KEY, 
            first_name varchar NOT NULL, 
            last_name varchar NOT NULL, 
            gender char(1), 
            level varchar NOT NULL);
""")

song_table_create = ("""
            CREATE TABLE IF NOT EXISTS songs (
            song_id varchar PRIMARY KEY, 
            title varchar NOT NULL, 
            artist_id varchar NOT NULL, 
            year int, 
            duration float);
""")

artist_table_create = ("""
            CREATE TABLE IF NOT EXISTS artists (
            artist_id varchar PRIMARY KEY, 
            name varchar NOT NULL, 
            location varchar, 
            latitude varchar, 
            longitude varchar);
""")

time_table_create = ("""
            CREATE TABLE IF NOT EXISTS time (
            start_time timestamp PRIMARY KEY, 
            hour int NOT NULL, 
            day int NOT NULL, 
            week int NOT NULL, 
            month int NOT NULL, 
            year int NOT NULL, 
            weekday int NOT NULL);
""")

# INSERT RECORDS

songplay_table_insert = ("""
            INSERT INTO songplays (
            start_time, 
            user_id, 
            level, 
            song_id, 
            artist_id, 
            session_id, 
            location, 
            user_agent) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
""")

user_table_insert = ("""
            INSERT INTO users (
            user_id, 
            first_name, 
            last_name, 
            gender, 
            level) 
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT(user_id)
            DO UPDATE
            SET level = EXCLUDED.level;
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
            DO NOTHING;
""")

# song_table_insert_cpy = ("""
# COPY songs(song_id, title, artist_id, year, duration) FROM \'/home/workspace/data/song_file.csv\' DELIMITER ',' CSV HEADER;
# """)



artist_table_insert = ("""
            INSERT INTO artists (
            artist_id, 
            name, 
            location, 
            latitude, 
            longitude) 
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (artist_id)
            DO NOTHING;
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

# FIND SONGS


song_select = ("""
            SELECT 
            songs.song_id, 
            artists.artist_id 
            FROM songs JOIN artists 
            ON songs.artist_id = artists.artist_id 
            WHERE songs.title = %s 
            AND artists.name = %s 
            AND songs.duration = %s AND songs.song_id is not null;
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]