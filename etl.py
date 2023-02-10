import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import MapType, StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Initializes the Spark Session. 
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Extracts the data from the S3 and load songs and artists information into parquet files on an S3. 
    """
    # get filepath to song data file
    song_data = input_data + "song_data/A/A/*/*.json"
    songSchema = StructType([StructField("num_songs",IntegerType()),\
                         StructField("artist_id",StringType()),\
                         StructField("artist_latitude",DoubleType()),\
                         StructField("artist_longitude",DoubleType()),\
                         StructField("artist_location",StringType()),\
                         StructField("artist_name",StringType()),\
                         StructField("song_id",StringType()),\
                         StructField("title",StringType()),\
                         StructField("duration",DoubleType()),\
                         StructField("year",IntegerType())\
                        ])
    
    # read song data file
    dfsong = spark.read.json(song_data,schema=songSchema,mode="DROPMALFORMED") 
    dfsong.createOrReplaceTempView("songs")
    
    # extract columns to create songs table
    songs_table = spark.sql("""
    SELECT song_id, title, artist_id, year, duration
    FROM songs
    """)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year").parquet(output_data + "songs/songs.parquet")

    # extract columns to create artists table
    artists_table = spark.sql("""
    SELECT 
        artist_id, 
        artist_name AS name, 
        artist_location AS location, 
        artist_latitude AS lattitude, 
        artist_longitude AS longitude
    FROM songs
    """)
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + "artists/artists.parquet")
    
    return dfsong


def process_log_data(spark, dfsong, input_data, output_data):
    """
    Extracts the data from the S3 and load times, users and songsplay into parquet files on an S3. 
    """
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json"

    # read log data file
    dflog = spark.read.json(log_data)
    dflog.createOrReplaceTempView("events")

    # extract columns for users table    
    users_table = spark.sql("""
    SELECT
        userid AS user_id, 
        firstname AS first_name, 
        lastname AS last_name, 
        gender, 
        level
    FROM events e
    WHERE
        e.page = 'NextSong'
    """)
    
    # write users table to parquet files
    users_table.write.parquet(output_data + "users/users.parquet")
    
    # extract columns to create time table
    time_table = spark.sql("""
    SELECT
        from_unixtime(e.ts/1000, "yyyy-MM-dd HH:mm:ss") AS start_time,
        hour(from_unixtime(e.ts/1000, "yyyy-MM-dd HH:mm:ss")) AS hour,
        day(from_unixtime(e.ts/1000, "yyyy-MM-dd HH:mm:ss")) AS day,        
        month(from_unixtime(e.ts/1000, "yyyy-MM-dd HH:mm:ss")) AS month,
        year(from_unixtime(e.ts/1000, "yyyy-MM-dd HH:mm:ss")) AS year,
        dayofweek(from_unixtime(e.ts/1000, "yyyy-MM-dd HH:mm:ss")) AS weekday
    FROM events e
    WHERE
        e.page = 'NextSong'
    """)
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').parquet(output_data + "times/times.parquet")

    # read in song data to use for songplays table
    #song_df = 
    dfsong.createOrReplaceTempView("songs")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
    SELECT
        row_number() over (order by "monotonically_increasing_id") as songplay_id,
        from_unixtime(e.ts/1000, "yyyy-MM-dd HH:mm:ss") AS start_time,
        e.userid AS user_id,
        e.level,
        s.song_id,
        s.artist_id,
        e.sessionid AS session_id,
        e.location,
        e.useragent
    FROM events e, songs s 
    WHERE
        e.page = 'NextSong' AND
        e.song = s.title AND
        e.artist = s.artist_name AND
        ABS(e.length - s.duration) < 2
    """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(output_data + "songplays/songplays.parquet")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://targetbucket/"
    
    dfsong = process_song_data(spark, input_data, output_data)    
    process_log_data(spark, dfsong, input_data, output_data)


if __name__ == "__main__":
    main()
