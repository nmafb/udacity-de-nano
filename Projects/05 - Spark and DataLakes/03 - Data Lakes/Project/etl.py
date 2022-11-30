import configparser
from datetime import datetime
import os
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql import functions as F
from pyspark.sql.types import DateType
from pyspark.sql.functions import *
from pyspark.sql.types import TimestampType
from pyspark.sql.types import TimestampType as TimeStamp



def create_spark_session():
    """
        This Function:
            - Creates an Spark Session based on the configurations
        Args:
            - None
        Returns:
            - spark (object): An activate local Spark Session
    """

    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):

    """
    This function:
        - Create songs and artists tables from data in S3 Bucket
        - Process it to load songs and artists tables
        - Write to partitioned parquet files on S3
    Args:
        - param: spark (spark.session): An active local Spark session
        - param: input_data (string): S3 bucket with input data
        - param: output_data (string): S3 bucket to store output tables (parquet files)
    Returns:
        - None
    """

    # get filepath to song data file
    print('input_date: {}', input_data)
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')

    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df['song_id', 'title', 'artist_id', 'year', 'duration']

    # write songs table to parquet files partitioned by year and artist

    songs_path = os.path.join(output_data, 'songs')
    songs_table.withColumn('_year', df.year).withColumn('_artist_id', df.artist_id) \
    .write.partitionBy(['_year', '_artist_id']) \
    .parquet(songs_path, mode='overwrite')

    # extract columns to create artists table
    artists_table = df['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']
    artists_table = artists_table.drop_duplicates(subset=['artist_id'])

    # write artists table to parquet files
    artists_path = os.path.join(output_data, 'artists')
    artists_table.write.parquet(artists_path, mode='overwrite')


def process_log_data(spark, input_data, output_data):
    """
    This Function:
        - Create users, time and songplay tables from data in S3 Bucket
        - Write to partitioned parquet files on S3
    Args:
        - param: spark (object session): An active Spark session
        - param: input_data (string): S3 bucket with input data
        - param: output_data (string): S3 bucket to store output tables
    Returns:
        - None
    """

    # get filepath to log data file
    log_data = os.path.join(input_data, 'log_data/*/*/*.json')

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df['userId', 'firstName', 'lastName', 'gender', 'level']
    users_table = users_table.drop_duplicates(subset=['userId'])
    
    # write users table to parquet files
    users_path = os.path.join(output_data, 'users')
    users_table.write.parquet(users_path, mode='overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(
        lambda x: datetime.fromtimestamp(x / 1000).replace(microsecond=0),
        TimestampType()
    )
    df = df.withColumn("timestamp", get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(
        lambda x: datetime.fromtimestamp(x / 1000).replace(microsecond=0),
        TimestampType()
    )
    df = df.withColumn("newdatetime", get_datetime(df.ts))
    
    # extract columns to create time table
    time_table = df.select(
        F.col('newdatetime').alias('start_time'),
        F.hour('newdatetime').alias('hour'),
        F.dayofmonth('newdatetime').alias('day'),
        F.weekofyear('newdatetime').alias('week'),
        F.month('newdatetime').alias('month'),
        F.year('newdatetime').alias('year'),
        F.date_format('newdatetime', 'u').alias('weekday')
    )
    time_table = time_table.drop_duplicates(subset=['start_time'])
    
    # write time table to parquet files partitioned by year and month
    time_path = os.path.join(output_data, 'time')
    time_table.write.partitionBy(['year', 'month']).parquet(time_path, mode='overwrite')

    # read in song data to use for songplays table
    song_path = os.path.join(output_data, 'songs/_year=*/_artist_id=*/*.parquet')
    song_df = spark.read.parquet(song_path)

    # extract columns from joined song and log datasets to create songplays table 
    df = df['datetime', 'userId', 'level', 'song', 'artist', 'sessionId', 'location', 'userAgent']
    log_song_df = df.join(song_df, df.song == song_df.title)
    
    songplays_table = log_song_df.select(
        F.monotonically_increasing_id().alias('songplay_id'),
        F.col('newdatetime').alias('start_time'),
        F.year('newdatetime').alias('year'),
        F.month('newdatetime').alias('month'),
        F.col('userId').alias('user_id'),
        F.col('level').alias('level'),
        F.col('song_id').alias('song_id'),
        F.col('artist_id').alias('artist_id'),
        F.col('sessionId').alias('session_id'),
        F.col('location').alias('location'),
        F.col('userAgent').alias('user_agent')
    )

    # write songplays table to parquet files partitioned by year and month
    songplays_path = os.path.join(output_data, 'songplays')
    songplays_table.write.partitionBy(['year', 'month']).parquet(songplays_path, mode='overwrite')


def main():
    """
        Main:
            - Extract songs and events data from S3
            - Transform it into tables
            - Load it back to S3 in Parquet format
        Args:
            - None
        Returns:
            - None
    """

    # Get Local Configuration
    config = configparser.ConfigParser()
    config.read('dl.cfg')

    os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']
    
    input_data = config['DATALAKE']['input_data']
    output_data = config['DATALAKE']['output_data']

    # Create Spark Session
    spark = create_spark_session()

    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')
    df = spark.read.json(song_data)
    
    # Process Song and Log data
    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
