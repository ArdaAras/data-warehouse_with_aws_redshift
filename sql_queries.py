import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')


# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events
                            (
                                artist VARCHAR,
                                auth VARCHAR,
                                firstName VARCHAR,
                                gender VARCHAR,
                                itemInSession INT,
                                lastName VARCHAR,
                                length FLOAT,
                                level VARCHAR,
                                location VARCHAR,
                                method VARCHAR,
                                page VARCHAR,
                                registration FLOAT,
                                sessionId INT,
                                song VARCHAR,
                                status INT,
                                ts VARCHAR,
                                userAgent VARCHAR,
                                userId INT
                            );
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs 
                            (
                                num_of_songs INT,
                                artist_id VARCHAR,
                                artist_name VARCHAR, 
                                artist_location VARCHAR,
                                artist_latitude FLOAT, 
                                artist_longitude FLOAT,
                                song_id VARCHAR,
                                title VARCHAR,
                                duration FLOAT,
                                year INT
                            );
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays
                            (
                                songplayId INT IDENTITY(0,1) PRIMARY KEY,
                                startTime TIMESTAMP NOT NULL,
                                userId    INT NOT NULL,
                                level      VARCHAR,
                                songId    VARCHAR,
                                artistId  VARCHAR,
                                sessionId INT,
                                location   VARCHAR,
                                userAgent VARCHAR,
                                UNIQUE (startTime, userId, songId, artistId)
                            );
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users
                        (
                            user_id    INT PRIMARY KEY,
                            first_name VARCHAR,
                            last_name  VARCHAR,
                            gender     VARCHAR,
                            level      VARCHAR
                        ); 
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs
                        (
                            song_id   VARCHAR PRIMARY KEY,
                            title     VARCHAR NOT NULL,
                            artist_id VARCHAR,
                            year      INT,
                            duration  FLOAT NOT NULL
                        );
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists 
                          (
                            artist_id VARCHAR PRIMARY KEY, 
                            name varchar NOT NULL, 
                            location VARCHAR, 
                            latitude FLOAT, 
                            longitude FLOAT
                          );
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time 
                        (
                            start_time TIMESTAMP PRIMARY KEY,
                            hour INT,
                            day INT, 
                            week INT, 
                            month INT, 
                            year INT, 
                            weekday VARCHAR
                        );
""")

# STAGING TABLES

staging_events_copy = ("""copy staging_events
                        from {}
                        credentials 'aws_iam_role={}'
                        region 'us-west-2'
                        json {}
""").format(config.get("S3", "LOG_DATA"),config.get("IAM_ROLE", "ARN"),config.get("S3", "LOG_JSONPATH"))

staging_songs_copy = ("""copy staging_songs
                        from {}
                        credentials 'aws_iam_role={}'
                        region 'us-west-2'
                        json 'auto'
""").format(config.get("S3", "SONG_DATA"),config.get("IAM_ROLE", "ARN"))

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays (
                                startTime,
                                userId,
                                level,
                                songId,
                                artistId,
                                sessionId,
                                location,
                                userAgent)
                            SELECT TIMESTAMP 'epoch' + (se.ts/1000 * INTERVAL '1 second'), \
                                    se.userId,se.level,se.song,se.artist,se.sessionId,se.location,se.userAgent
                            FROM staging_events se, staging_songs ss
                            WHERE se.song = ss.title 
                                AND se.artist = ss.artist_name 
                                AND se.length = ss.duration
                                AND se.page = 'NextSong'
""")

user_table_insert = ("""INSERT INTO users(
                            user_id,
                            first_name,
                            last_name,
                            gender,
                            level)
                        SELECT DISTINCT userId,firstName,lastName,gender,level
                        FROM staging_events
                        WHERE userId IS NOT NULL;
""")

song_table_insert = ("""INSERT INTO songs(
                            song_id,
                            title,
                            artist_id,
                            year,
                            duration)
                        SELECT DISTINCT song_id,title,artist_id,year,duration
                        FROM staging_songs
                        WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""INSERT INTO artists(
                            artist_id, 
                            name, 
                            location, 
                            latitude, 
                            longitude)
                          SELECT DISTINCT artist_id,artist_name,artist_location,artist_latitude,artist_longitude
                          FROM staging_songs
                          WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""INSERT INTO time(start_time,hour,day,week,month,year,weekday)
                        SELECT startTime,
                                EXTRACT(h from startTime) as hour,
                                EXTRACT(d from startTime) as day,
                                EXTRACT(w from startTime) as week,
                                EXTRACT(mon from startTime) as month,
                                EXTRACT(y from startTime) as year,
                                EXTRACT(dow from startTime) as weekday
                        FROM songplays;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]