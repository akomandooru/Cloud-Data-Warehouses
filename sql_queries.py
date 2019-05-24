import configparser


# CONFIG - setup the configuration reader so we can use the settings later on 
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES - drop the tables if they exist to start the ETL process from scratch
staging_events_table_drop = "DROP TABLE IF EXISTS \"staging_events\";"
staging_songs_table_drop = "DROP TABLE IF EXISTS \"staging_songs\";"
songplay_table_drop = "DROP TABLE IF EXISTS \"songplay\";"
user_table_drop = "DROP TABLE IF EXISTS \"users\";"
song_table_drop = "DROP TABLE IF EXISTS \"song\";"
artist_table_drop = "DROP TABLE IF EXISTS \"artist\";"
time_table_drop = "DROP TABLE IF EXISTS \"time\";"


# CREATE TABLES
#create staging_events table for collecting the log data
staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events (\
                                artist text,\
                                auth text,\
                                firstname text,\
                                gender char(1),\
                                iteminsession integer,\
                                lastname text,\
                                length float,\
                                level text,\
                                location text,\
                                method char(3),\
                                page text,\
                                registration float,\
                                sessionid integer,\
                                song text,\
                                status integer,\
                                ts bigint not null,\
                                useragent text,\
                                userid text);""")

#create staging_songs table for collecting song data
staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs (\
                                num_songs int,\
                                artist_id text,\
                                artist_latitude float,\
                                artist_longitude float,\
                                artist_location text,\
                                artist_name text,\
                                song_id text,\
                                title text,\
                                duration float,\
                                year integer);""")

#create songplay table to use as a fact table; setup references to other dimension tables; also, setup song_id as a distribution key
songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplay (start_time bigint not null references time(start_time),\
                         user_id integer not null references users(user_id),\
                         level text not null,\
                         song_id text sortkey distkey references song(song_id), \
						 artist_id text references artist(artist_id),\
                         session_id integer not null, \
                         location text,\
                         user_agent text);""")

#create users dimension table to capture user information; set the table to distributed across all nodes
user_table_create = ("""CREATE TABLE IF NOT EXISTS users (user_id integer not null sortkey primary key, first_name text NOT NULL, \
					 last_name text NOT NULL, \
                     gender char(1) not null, \
                     level text NOT NULL)\
                     diststyle all;""")

#create song table to keep song information; this dimension table could be big and so set its song_id column as distribution key
song_table_create = ("""CREATE TABLE IF NOT EXISTS song (song_id text not null distkey primary key, title text NOT NULL, \
                    artist_id text NOT NULL, year integer NOT NULL, duration float NOT NULL);""")

#create artist dimension table to collect artist information; set the table to be distributed/copied on each node
artist_table_create = ("""CREATE TABLE IF NOT EXISTS artist (artist_id text not null sortkey primary key, \
                       name text NOT NULL,location text NOT NULL,lattitude float, \
                       longitude float)\
                       diststyle all;""")

#create time dimension table for keeping time information; distribute the table across all nodes 
time_table_create = ("""CREATE TABLE IF NOT EXISTS time (start_time bigint NOT NULL sortkey primary key, hour integer NOT NULL, \
                    day integer NOT NULL, week integer NOT NULL, \
                    month integer NOT NULL, year integer NOT NULL, weekday integer NOT NULL)\
                    diststyle all;""")

# STAGING TABLES
"""
    Staging table for events data needs special handlings as described below:
    Need to use a jsonpath file for copying to the staging events table; Redshift column names are lowercase 
    but the property names in our log data files are mixed case; 
"""
staging_events_copy = ("""copy staging_events from {} \
                       iam_role {} \
                       region 'us-west-2' FORMAT AS JSON {};\
                       """).format(config.get("S3","LOG_DATA"),config.get("IAM_ROLE","ARN"),config.get("S3","LOG_JSONPATH"))

staging_songs_copy = ("""copy staging_songs from {} \
                       iam_role {} \
                       region 'us-west-2' FORMAT AS JSON 'auto';\
                       """).format(config.get("S3","SONG_DATA"),config.get("IAM_ROLE","ARN"))


# FINAL TABLES
#select by left joining staging_events with staging_songs table and filter the entries by 'NextSong' to populate songplay table
songplay_table_insert = ("""insert into songplay \
                            (select ts as start_time, cast(userid as integer) as user_id,\
                            level, song_id, artist_id, sessionid as session_id,\
                            location, useragent as user_agent \
                            from staging_events \
                            left outer join staging_songs \
                            on staging_events.artist = staging_songs.artist_name \
                            and staging_events.song = staging_songs.title \
                            where staging_events.page = 'NextSong')\
                        """)

#select from staging_events table to insert into users table
user_table_insert = ("""insert into users \
                        (select cast(userid as integer) as user_id, min(firstname) as first_name,\
                        min(lastname) as last_name, min(gender) as gender, min(level) as level \
                        from staging_events \
                        group by userid having user_id is not null)\
                    """)

#select from staging_songs table to insert into song table
song_table_insert = ("""insert into song \
                        (select song_id, min(title),\
                        min(artist_id), min(year), min(duration) \
                        from staging_songs \
                        group by song_id having song_id is not null)\
                    """)

#select from staging_songs table to insert into artist table
artist_table_insert = ("""insert into artist \
                          (select artist_id, min(artist_name) as name,\
                          min(artist_location) as location,\
                          min(artist_latitude) as lattitude,\
                          min(artist_longitude) as longitude \
                          from staging_songs \
                          group by artist_id having artist_id is not null)\
                        """)

#select from staging_events table to insert into time table
time_table_insert = ("""insert into time \
                        (select ts as starttime, date_part(h,timestamp 'epoch' + ts/1000 * interval '1 second') as hour,\
                        date_part(day,timestamp 'epoch' + ts/1000 * interval '1 second') as day,\
                        date_part(w,timestamp 'epoch' + ts/1000 * interval '1 second') as week,\
                        date_part(mon,timestamp 'epoch' + ts/1000 * interval '1 second') as month,\
                        date_part(y,timestamp 'epoch' + ts/1000 * interval '1 second') as year,\
                        date_part(dow,timestamp 'epoch' + ts/1000 * interval '1 second') as weekday \
                        from staging_events \
                        group by ts having ts is not null)\
                    """)

# QUERY LISTS
#list of create table script 
create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
#list of drop table script 
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
#list of copy table script to copy from S3 storage to Redshift
copy_table_queries = [staging_events_copy, staging_songs_copy]
#list of insert table script to copy from staging tables to fact and dimension tables
insert_table_queries = [songplay_table_insert,user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
