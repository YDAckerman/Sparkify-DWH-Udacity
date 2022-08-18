import configparser

config_path = 'dwh.cfg'
config = configparser.ConfigParser()
config.read_file(open(config_path))

# DROP SCHEMAS

bi_schema_drop = "DROP SCHEMA IF EXISTS bi_schema CASCADE"
staging_schema_drop = "DROP SCHEMA IF EXISTS staging_schema CASCADE"

# CREATE SCHEMAS

bi_schema_create = "CREATE SCHEMA IF NOT EXISTS bi_schema"
staging_schema_create = "CREATE SCHEMA IF NOT EXISTS staging_schema"

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_schema.events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_schema.songs"
songplay_table_drop = "DROP TABLE IF EXISTS bi_schema.songplay"
user_table_drop = "DROP TABLE IF EXISTS bi_schema.users"
song_table_drop = "DROP TABLE IF EXISTS bi_schema.songs"
artist_table_drop = "DROP TABLE IF EXISTS bi_schema.artists"
time_table_drop = "DROP TABLE IF EXISTS bi_schema.time"

# CREATE TABLES

staging_events_table_create = """
CREATE TABLE IF NOT EXISTS staging_schema.events (
artist               TEXT,
auth                 VARCHAR,
firstName            VARCHAR,
gender               VARCHAR,
itemInSession        INT,
lastName             VARCHAR,
length               REAL,
level                VARCHAR,
location             TEXT,
method               VARCHAR,
page                 VARCHAR,
registration         FLOAT,
sessionId            INT,
song                 TEXT,
status               INT,
ts                   BIGINT,
userAgent            TEXT,
userId               INT
);
"""

staging_songs_table_create = """
CREATE TABLE IF NOT EXISTS staging_schema.songs (
num_songs            INT,
artist_id            VARCHAR,
artist_latitude      FLOAT,
artist_longitude     FLOAT,
artist_location      TEXT,
artist_name          TEXT,
song_id              VARCHAR,
title                TEXT,
duration             REAL,
year                 INT
)
"""

songplay_table_create = """
CREATE TABLE IF NOT EXISTS bi_schema.songplay (
songplay_id   INT       IDENTITY(0,1)      PRIMARY KEY    DISTKEY,
start_time    TIMESTAMP                    NOT NULL       SORTKEY,
user_id       INT                          NOT NULL,
level         VARCHAR,
song_id       VARCHAR,
artist_id     VARCHAR,
session_id    INT                          NOT NULL,
location      TEXT,
user_agent    TEXT
);
"""

user_table_create = """
CREATE TABLE IF NOT EXISTS bi_schema.users (
user_id    INT PRIMARY KEY SORTKEY,
first_name VARCHAR,
last_name  VARCHAR,
gender     VARCHAR,
level      VARCHAR
) diststyle all;
"""

song_table_create = """
CREATE TABLE IF NOT EXISTS bi_schema.songs (
song_id   VARCHAR PRIMARY KEY SORTKEY,
title     TEXT    NOT NULL,
artist_id VARCHAR NOT NULL,
year      INT     NOT NULL,
duration  REAL    NOT NULL
) diststyle all;
"""

artist_table_create = """
CREATE TABLE IF NOT EXISTS bi_schema.artists (
artist_id           VARCHAR PRIMARY KEY SORTKEY,
name                TEXT    NOT NULL,
location            TEXT,
latitude            FLOAT,
longitude           FLOAT
) diststyle all;
"""

time_table_create = """
CREATE TABLE IF NOT EXISTS bi_schema.time (
start_time TIMESTAMP PRIMARY KEY SORTKEY,
hour    INT             NOT NULL,
day     INT             NOT NULL,
week    INT             NOT NULL,
weekday VARCHAR         NOT NULL,
month   INT             NOT NULL,
year    INT             NOT NULL
) diststyle all;
"""


# STAGING TABLES

staging_events_copy = ("""
COPY staging_schema.events FROM '{}'
CREDENTIALS 'aws_iam_role={}'
json '{}'
region 'us-west-2';
""").format(config.get('S3', 'LOG_DATA'),
            config.get('DWH', 'ROLE_ARN'),
            config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = ("""
COPY staging_schema.songs FROM '{}'
CREDENTIALS 'aws_iam_role={}'
json 'auto'
region 'us-west-2';
""").format(config.get('S3', 'SONG_DATA'),
            config.get('DWH', 'ROLE_ARN'))

# FINAL TABLES

songplay_table_insert = """INSERT INTO bi_schema.songplay
(start_time, user_id, level, song_id,
 artist_id, session_id, location, user_agent)
SELECT timestamp 'epoch' + se.ts/1000 * interval '1 second',
       se.userId,
       se.level,
       ss.song_id,
       ss.artist_id,
       se.sessionId,
       se.location,
       se.userAgent
FROM staging_schema.events se
JOIN staging_schema.songs ss
ON se.artist = ss.artist_name AND se.song = ss.title
WHERE se.page = 'NextSong';
"""

# solution from:
# https://stackoverflow.com/questions/28085468/
user_table_insert = """INSERT INTO bi_schema.users
(user_id, first_name, last_name, gender, level)
SELECT userId, firstName, lastName, gender, level
FROM (
SELECT userId, firstName, lastName, gender, level,
       row_number() OVER (PARTITION BY userId ORDER BY ts DESC) AS rn
FROM staging_schema.events
WHERE page = 'NextSong'
)
WHERE rn = 1
"""

song_table_insert = """INSERT INTO bi_schema.songs
(song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id, title, artist_id, year, duration
FROM staging_schema.songs;
"""

artist_table_insert = """INSERT INTO bi_schema.artists
(artist_id, name, location, latitude, longitude)
SELECT DISTINCT artist_id, artist_name,
                artist_location, artist_latitude,
                artist_longitude
FROM staging_schema.songs;
"""

# solution from:
# https://stackoverflow.com/a/36399361
time_table_insert = """INSERT INTO bi_schema.time
(start_time, hour, day, week, weekday, month, year)
SELECT start_time,
       date_part(hr, start_time),
       date_part(d, start_time),
       date_part(w, start_time),
       date_part(dw, start_time),
       date_part(mon, start_time),
       date_part(yr, start_time)
FROM (
SELECT DISTINCT timestamp 'epoch' + ts/1000 * interval '1 second'
                AS start_time
FROM staging_schema.events
WHERE page = 'NextSong'
);
"""


# QUERY LISTS

create_schema_queries = [bi_schema_create,
                         staging_schema_create]

drop_schema_queries = [bi_schema_drop,
                       staging_schema_drop]

create_table_queries = [staging_events_table_create,
                        staging_songs_table_create,
                        songplay_table_create,
                        user_table_create,
                        song_table_create,
                        artist_table_create,
                        time_table_create
                        ]

drop_table_queries = [staging_events_table_drop,
                      staging_songs_table_drop,
                      songplay_table_drop,
                      user_table_drop,
                      song_table_drop,
                      artist_table_drop,
                      time_table_drop]

copy_table_queries = [staging_events_copy,
                      staging_songs_copy]

insert_table_queries = [user_table_insert,
                        song_table_insert,
                        artist_table_insert,
                        time_table_insert,
                        songplay_table_insert
                        ]
