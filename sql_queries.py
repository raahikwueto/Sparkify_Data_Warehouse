import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

DWH_ROLE_ARN = config.get('IAM_ROLE', 'ARN')
LOG_DATA = config.get('S3', 'LOG_DATA')
LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
SONG_DATA=config.get('S3', 'SONG_DATA')
# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
                              eventId BIGINT IDENTITY(0, 1) NOT NULL,
                              artist VARCHAR NULL,
                              auth VARCHAR NULL,
                              firstName VARCHAR NULL,
                              gender VARCHAR NULL,
                              itemInSession INTEGER NULL,
                              lastname VARCHAR NULL,
                              length double precision NULL,
                              level VARCHAR NULL,
                              location VARCHAR NULL,
                              method VARCHAR NULL,
                              page VARCHAR NULL,
                              registration VARCHAR NULL,
                              sessionId INTEGER NOT NULL,
                              song VARCHAR NULL,
                              status INTEGER NULL,
                              ts BIGINT NOT NULL,
                              userAgent VARCHAR NULL,
                              userId INTEGER NULL
)
DISTSTYLE KEY
DISTKEY(sessionId)
SORTKEY(sessionId);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs INTEGER NULL,
    artist_id VARCHAR NOT NULL,
    artist_latitude VARCHAR NULL,
    artist_longitude VARCHAR NULL,
    artist_location VARCHAR NULL,
    artist_name VARCHAR NULL,
    song_id VARCHAR NOT NULL,
    title VARCHAR NULL,
    duration DOUBLE PRECISION NULL,
    year INTEGER NULL
)
DISTSTYLE KEY
DISTKEY(artist_id)
SORTKEY(artist_id);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id BIGINT IDENTITY(0,1) PRIMARY KEY,
    start_time TIMESTAMP NOT NULL, 
    user_id INTEGER NOT NULL REFERENCES users(user_id),
    level VARCHAR(5) NOT NULL,
    song_id VARCHAR(50) NOT NULL REFERENCES songs(song_id),
    artist_id VARCHAR(50) REFERENCES artists(artist_id),
    session_id VARCHAR(50) NOT NULL,
    location VARCHAR(100) NULL,
    user_agent VARCHAR(512) NULL
)
DISTSTYLE KEY
DISTKEY (songplay_id)
SORTKEY (user_id);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY SORTKEY, 
    first_name VARCHAR(256) NULL, 
    last_name VARCHAR(256) NULL, 
    gender VARCHAR(6) NULL, 
    level VARCHAR(5) NULL
)
DISTSTYLE ALL;
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id VARCHAR(50) PRIMARY KEY SORTKEY, 
    title VARCHAR(500) NOT NULL, 
    artist_id VARCHAR(50) NOT NULL REFERENCES artists(artist_id),
    year INTEGER NOT NULL, 
    duration DOUBLE PRECISION NOT NULL
)
DISTSTYLE ALL;
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR(50) PRIMARY KEY SORTKEY, 
    name VARCHAR(255) NULL, 
    location VARCHAR(1000) NULL, 
    latitude VARCHAR NULL, 
    longitude VARCHAR NULL
)
DISTSTYLE ALL;
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP PRIMARY KEY SORTKEY, 
    hour INTEGER NULL, 
    day INTEGER NULL, 
    week INTEGER NULL, 
    month INTEGER NULL, 
    year INTEGER NULL, 
    weekday INTEGER NULL
)
DISTSTYLE ALL;
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events 
FROM {}
FORMAT AS JSON {}
CREDENTIALS 'aws_iam_role={}'
REGION 'us-west-2'
STATUPDATE ON 
TIMEFORMAT AS 'epochmillisecs'
TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
""").format(LOG_DATA, LOG_JSONPATH, DWH_ROLE_ARN)

staging_songs_copy = ("""
COPY staging_songs 
FROM {}
FORMAT AS JSON 'auto'
CREDENTIALS 'aws_iam_role={}'
REGION 'us-west-2'
STATUPDATE OFF 
TRUNCATECOLUMNS 
BLANKSASNULL 
EMPTYASNULL 
ACCEPTINVCHARS AS '^'
MAXERROR 10
COMPUPDATE OFF;
""").format(SONG_DATA, DWH_ROLE_ARN)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays ( start_time, 
                         user_id, 
                         level, 
                         song_id, 
                         artist_id, 
                         session_id, 
                         location, 
                         user_agent)
SELECT 
    TIMESTAMP 'epoch' + e.ts/1000 * INTERVAL '1 second' AS start_time,
    e.userId AS user_id,
    e.level AS level,
    s.song_id AS song_id,
    s.artist_id AS artist_id,
    e.sessionId AS session_id,
    e.location AS location,
    e.userAgent AS user_agent
FROM staging_events e
JOIN staging_songs s ON (e.artist = s.artist_name And e.length = s.duration)
WHERE e.page = 'NextSong' AND e.userId is not NULL;

""")

user_table_insert = ("""
INSERT INTO users (user_id, 
                     first_name, 
                     last_name, 
                     gender, 
                     level)
SELECT DISTINCT e.userId AS user_id, 
                     e.firstName AS first_name, 
                     e.lastName AS last_name, 
                     e.gender AS gender, 
                     e.level as level
FROM staging_events e
WHERE e.page = 'NextSong' AND e.userId is not NULL;
""")

song_table_insert = ("""
INSERT INTO songs (song_id, 
                     title, 
                     artist_id, 
                     year, 
                     duration)
SELECT DISTINCT s.song_id AS song_id, 
                     s.title AS title, 
                     s.artist_id AS artist_id, 
                     s.year AS year, 
                     s.duration AS duration
FROM staging_songs s
WHERE s.song_id IS NOT NULL;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id,
                        name, 
                       location, 
                       latitude, 
                       longitude)
SELECT DISTINCT artist_id AS artist_id,
                        artist_name AS name, 
                       artist_location AS location, 
                       artist_latitude AS latitude, 
                       artist_longitude AS longitude
FROM staging_songs
WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
INSERT INTO time (start_time, 
                     hour, 
                     day, 
                     week, 
                     month, 
                     year, 
                     weekday)
SELECT DISTINCT TIMESTAMP 'epoch' + e.ts/1000 * INTERVAL '1 second' AS start_time, 
                     EXTRACT(HOUR FROM start_time) AS hour, 
                     EXTRACT(DAY FROM start_time) AS day, 
                     EXTRACT(WEEK FROM start_time) as week, 
                     EXTRACT(MONTH FROM start_time) as month, 
                     EXTRACT(YEAR FROM start_time) as year, 
                     EXTRACT(DOW FROM start_time) as weekday
FROM staging_events e
WHERE e.page = 'NextSong' AND e.userId is not NULL ;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, time_table_create, user_table_create, artist_table_create, song_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, time_table_drop, user_table_drop, song_table_drop, artist_table_drop, songplay_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert,time_table_insert, user_table_insert, artist_table_insert, song_table_insert]

# SELECT QUERIES
highest_usage_time_of_day = (""" SELECT t.hour AS time_of_day, sum(ss.duration)/3600 AS hour_spent 
                FROM songplays sp
                JOIN time t on (sp.start_time = t.start_time)
                JOIN songs ss on (ss.song_id = sp.song_id)
                GROUP BY time_of_day
                ORDER BY hour_spent DESC;
                """)

most_played_song = (""" SELECT s.title AS Song_title, a.name as Artist_name, count(sp.songplay_id) as frequency
                    FROM songplays sp
                    JOIN songs s ON (s.song_id = sp.song_id)
                    JOIN artists a ON (a.artist_id = sp.artist_id)
                    GROUP BY song_title, artist_name
                    ORDER BY frequency DESC
                    LIMIT 10;
                    """)

