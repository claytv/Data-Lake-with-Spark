import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql import types as t


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Creates a spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    
    """
    *Inputs*
     spark - Spark session
     input_data - Location of data to read (S3 Bucket or Local)
     output_data - Location for tables to be written to (S3 Bucket or Local)
    
    *Description*
     Reads song data from input_data and write song and artists tables to output_data
    
    """
    
    print("** Processing song data **")
    
    song_data = input_data + 'song_data/*/*/*/*.json'
    #song_data = 'data/starter_song_data/*/*/*/*.json'
    
    # read song data file
    df_song = spark.read.json(song_data).dropDuplicates()

    # extract columns to create songs table
    df_song.createOrReplaceTempView("songs_table")
    
    songs_table = spark.sql("""
        SELECT song_id, title, artist_id, year, duration
        FROM songs_table
        ORDER BY song_id 
    """)
    
    # write songs table to parquet files partitioned by year and artist
    print("Writing song_table...")
    
    songs_table.write.mode("overwrite").partitionBy("year", "artist_id").parquet(output_data + 'song_table.parquet')
    
    # extract columns to create artists table
    artists_table = spark.sql("""
    SELECT artist_id          AS artist_id,
           artist_name        AS name,
           artist_location    AS location, 
           artist_latitude    AS latitude, 
           artist_longitude   AS longitude 
    FROM songs_table 
    ORDER by artist_id
    """)
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(output_data + 'artist_table.parquet')
    
    print("Writing artist_table...")


def process_log_data(spark, input_data, output_data):
    
    """
    *Inputs*
     spark - Spark session
     input_data - Location of data to read (S3 Bucket or Local)
     output_data - Location for tables to be written to (S3 Bucket or Local)
    
    *Description*
     Reads log data from input_data and user, time and songplay tables to output_data
    
    """
    
    # get filepath to log data file
    
    print("** Processing log data **")
    
    log_data = input_data + "log_data/*/*/*.json"
    #log_data = 'data/starter_log_data/*/*/*.json'

    # read log data file
    df_log = spark.read.json(log_data).dropDuplicates()
    
    # filter by actions for song plays
    df_log_sp_filter = df_log.filter(df_log.page == 'NextSong')

    # extract columns for users table 
    df_log_sp_filter.createOrReplaceTempView("users_table")
    ### user_id, first_name, last_name, gender, level
    users_table = spark.sql("""
        SELECT  DISTINCT userId    AS user_id, 
                     firstName AS first_name, 
                     lastName  AS last_name, 
                     gender, 
                     level
        FROM users_table
        ORDER BY last_name
    """)
    
    # write users table sto parquet files
    print("Writing users_table...")
    users_table.write.mode("overwrite").parquet(output_data + 'users_table.parquet')
    
    # create timestamp column from original timestamp column
    @udf(t.TimestampType())
    def get_timestamp (ts):
        return datetime.fromtimestamp(ts / 1000.0)

    df_log_filter = df_log_sp_filter.withColumn("timestamp", get_timestamp("ts"))
    
    # create datetime column from original timestamp column
    @udf(t.StringType())
    def get_datetime(ts):
        return datetime.fromtimestamp(ts / 1000.0).strftime('%Y-%m-%d %H:%M:%S')
    
    # create datetime column from original timestamp column
    df_log_filter = df_log_filter.withColumn("datetime", get_datetime("ts"))
    
    # extract columns to create time table
    df_log_filter.createOrReplaceTempView("time_table")

    time_table = spark.sql("""
        SELECT DISTINCT datetime            AS start_time,
                    hour(timestamp)         AS hour,
                    day(timestamp)          AS day,
                    weekofyear(timestamp)   AS week,
                    month(timestamp)        AS month,
                    year(timestamp)         AS year, 
                    dayofweek(timestamp)    AS weekday
        FROM time_table
        ORDER BY start_time
    """)
    
    # write time table to parquet files partitioned by year and month
    print("Writing time_table...")
    time_table.write.mode("overwrite").partitionBy("year", "month").parquet(output_data + 'time_table.parquet')
    
    song_data = input_data + 'song_data/*/*/*/*.json'
    #song_data = 'data/starter_song_data/*/*/*/*.json'
    
    df_song = spark.read.json(song_data).dropDuplicates()
    # read in song data to use for songplays table
    df_log_sd_join = df_log_filter.join(df_song, (df_log_filter.artist == df_song.artist_name) & (df_log_filter.song == df_song.title))
    df_log_sd_join = df_log_sd_join.withColumn("songplay_id", monotonically_increasing_id())

    # extract columns from joined song and log datasets to create songplays table 
    df_log_sd_join.createOrReplaceTempView("songplays_table")
    songplays_table = spark.sql("""
    SELECT songplay_id      AS songplay_id,
           timestamp        AS start_time,  
           year(timestamp)  AS year, 
           month(timestamp) AS month,
           userId           AS user_id,
           level            AS level,
           song_id          AS song_id, 
           artist_id        AS artist_id, 
           sessionId        AS session_id,
           location         AS location,
           userAgent        AS user_agent
    FROM songplays_table
    ORDER BY (user_id, session_id)

    """)
    
    # write songplays table to parquet files partitioned by year and month
    print("Writing songplays_table...")
    songplays_table.write.mode("overwrite").partitionBy("year", "month").parquet(output_data + 'songplays_table.parquet')


def main():
    spark = create_spark_session()
    input_data = config['AWS']['INPUT_DATA']
    output_data = config['AWS']['OUTPUT_DATA']
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
