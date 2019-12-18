import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE staging_events (
    artist           VARCHAR,
    auth             VARCHAR,
    firstName        VARCHAR,
    gender           VARCHAR,
    itemInSession    INTEGER,
    lastName         VARCHAR,
    length           FLOAT,
    level            VARCHAR,
    location         VARCHAR,
    method           VARCHAR,
    page             VARCHAR,
    registration     FLOAT,
    sessionId        INTEGER,
    song             VARCHAR,
    status           INTEGER,
    ts               BIGINT,
    userAgent        VARCHAR,
    userId           INTEGER
);"""
)

staging_songs_table_create = ("""
CREATE TABLE staging_songs (
    artist_id        VARCHAR,
    artist_latitude  FLOAT,
    artist_longitude FLOAT,
    artist_location  VARCHAR,
    artist_name      VARCHAR,
    duration         FLOAT,
    num_song         INTEGER,
    song_id          VARCHAR,
    title            VARCHAR,
    year             INTEGER
);"""
)


songplay_table_create = ("""
CREATE TABLE songplay (
    songplay_id    INT IDENTITY(1, 1) PRIMARY KEY,
    start_time     TIMESTAMP NOT NULL sortkey,
    user_id        INTEGER NOT NULL distkey,
    level          VARCHAR,
    song_id        VARCHAR NOT NULL,
    artist_id      VARCHAR NOT NULL,
    session_id     INTEGER NOT NULL,
    location       VARCHAR NOT NULL,
    user_agent     VARCHAR NOT NULL
);"""
)


user_table_create = ("""
CREATE TABLE users (
    user_id      INTEGER NOT NULL PRIMARY KEY,
    first_name   VARCHAR NOT NULL,
    last_name    VARCHAR NOT NULL,
    gender       VARCHAR NOT NULL,
    level        VARCHAR NOT NULL
) diststyle all;"""
)


song_table_create = ("""
CREATE TABLE song (
    SONG_ID     VARCHAR NOT NULL PRIMARY KEY,
    title       VARCHAR NOT NULL,
    ARTIST_ID   VARCHAR NOT NULL,
    YEAR        INTEGER NOT NULL,
    DURATION    INTEGER NOT NULL
    );
""")

artist_table_create = ("""
CREATE TABLE artists (
    artist_id    VARCHAR NOT NULL PRIMARY KEY,
    name         VARCHAR,
    location     VARCHAR,
    lattitude    REAL,
    longitude    REAL
);"""
)

time_table_create = ("""
CREATE TABLE time (
    START_TIME   TIMESTAMP NOT NULL PRIMARY KEY,
    HOUR         INT NOT NULL,
    DAY          VARCHAR NOT NULL,
    WEEK         VARCHAR NOT NULL,
    MONTH        VARCHAR NOT NULL,
    YEAR         INTEGER NOT NULL,
    WEEKDAY      VARCHAR NOT NULL
    );
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events
    from {}
    iam_role {}
    json {}
""").format(config.S3.LOG_DATA,
            config.IAM_ROLE.ARN,
            config.S3.LOG_JSONPATH)


staging_songs_copy = ("""
    copy staging_songs
    from {}
    iam_role {}
    json 'auto'
""").format(config.S3.SONG_DATA,
            config.IAM_ROLE.ARN)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplay(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT       TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1second' AS start_time,
             userId AS user_id,
             level AS level,
             song_id AS song_id,
             artist_id AS artist_id,
             sessionid AS Session_id,
             location AS location,
             userAgent AS user_agent
FROM staging_events AS se, staging_songs AS ss
WHERE se.artist = ss.artist_name
AND se.song = ss.title
AND page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO users(user_id, first_name, last_name, gender, level)
SELECT userId AS user_id,
       firstName AS first_name,
       lastName AS last_name,
       gender AS gender,
       level AS level
FROM staging_events AS se
WHERE page = 'NextSong'
AND userid NOT IN (
    SELECT DISTINC(user_Id) 
    FROM users AS u
    WHERE u.user_id = se.userid
    )
GROUP BY userid, firstname, lastname, gender, level;
""")

song_table_insert = ("""
INSERT INTO songs(song_id, title, artist_id, year, duration)
SELECT song_id AS song_id, 
       title AS title,
       artist_id AS artist_id,
       year AS year,
       duration AS duration
FROM staging_songs;
""")

artist_table_insert = ("""
INSERT INTO artists(artist_id, name, location, lattitude, longitude)
SELECT artist_id AS artist_id,
       artist_name AS name,
       artist_location AS location,
       artist_latitude AS lattitude,
       artist_longitude AS longitude
FROM staging_songs AS ss
WHERE artist_id NOT IN
(
    SELECT DISTINC (artist_id) 
    FROM artists AS a
    WHERE a.artist_id = ss.artist_id
)
GROUP BY artist_id, artist_name, artist_location, artist_latitude, artist_longitude;
""")

time_table_insert = ("""
INSERT INTO time(time_key, start_time, hour, day, week, month, year, weekday)
SELECT ts AS time_key,
             (TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second') AS start_time,
             EXTRACT (h FROM (TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second')) AS hour,
             EXTRACT (d FROM (TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second')) AS day,
             EXTRACT (w FROM (TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second')) as Week,
             EXTRACT (mon FROM (TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second')) AS month,
             EXTRACT (year FROM (TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second')) AS year,
             EXTRACT (dow FROM (TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second')) AS weekday
FROM staging_events;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
