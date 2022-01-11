import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Creates a Spark session.
    """
    spark = SparkSession \
        .builder \
        .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:2.7.0') \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Ingests data from S3, then extracts and loads data into songs_table and artists_table.
    
    Parameters:
        spark: The Spark session object.
        input_data: The location of the song_data files to parse
        output_data: The location where we want to write the new tables
    """
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # define schema
    song_schema = StructType([
        StructField('artist_id', StringType(), False),
        StructField('artist_latitude', DoubleType(), True),
        StructField('artist_location', StringType(), True),
        StructField('artist_longitude', DoubleType(), True),
        StructField('artist_name', StringType(), True),
        StructField('duration', DoubleType(), True),
        StructField('num_songs', IntegerType(), True),
        StructField('song_id', StringType(), False),
        StructField('title', StringType(), False),
        StructField('year', IntegerType(), False)
    ])
    
    # read song data file
    df_song = spark.read.csv(song_data, schema=song_schema)

    # extract columns to create songs table
    songs_table = df_song.select('song_id', 'title', 'artist_id', 'year', 'duration').dropDuplicates() 
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy('year', 'artist_id').parquet(output_data + 'songs_table/')

    # extract columns to create artists table
    artists_table = df_song.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude').dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data + 'artists_table/')

    # create a view to make loading and reading easier later
    df_song.createOrReplaceTempView('df_song_table')

def process_log_data(spark, input_data, output_data):
    """
    Ingests data from S3, then extracts, transforms and loads data into users_table, time_table and the songplays_table fact table.
    
    Parameters:
        spark: The Spark session object.
        input_data: The location of the song_data files to parse
        output_data: The location where we want to write the new tables
    """
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'

    # define schema
    log_schema = StructType([
        StructField('artist', StringType(), False),
        StructField('auth', StringType(), True),
        StructField('firstName', StringType(), True),
        StructField('gender', StringType(), True),
        StructField('itemInSession', IntegerType(), True),
        StructField('lastName', StringType(), True),
        StructField('length', DoubleType(), True),
        StructField('level', StringType(), False),
        StructField('location', StringType(), False),
        StructField('method', StringType(), True),
        StructField('page', StringType(), True),
        StructField('registration', DoubleType(), True),
        StructField('sessionId', IntegerType(), False),
        StructField('song', StringType(), True),
        StructField('status', IntegerType(), True),
        StructField('ts', LongType(), False),
        StructField('userAgent', StringType(), False),
        StructField('userId', StringType(), False)
    ])
    
    # read log data file
    df = spark.read.json(log_data, schema=log_schema)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')
    
    # extract columns for users table    
    users_table = df.select('userId', 'firstName', 'lastName', 'gender', 'level').dropDuplicates()

    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data + 'users_table/')

    # create datetime column from original timestamp column
    get_timestamp = udf(lambda x : datetime.utcfromtimestamp(int(x)/1000), TimestampType())
    df = df.withColumn('start_time', get_timestamp('ts'))
    
    # extract columns to create time table
    time_table = df.withColumn('hour',hour('start_time')) \
                   .withColumn('day',dayofmonth('start_time')) \
                   .withColumn('week',weekofyear('start_time')) \
                   .withColumn('month',month('start_time')) \
                   .withColumn('year',year('start_time')) \
                   .withColumn('weekday',dayofweek('start_time')) \
                   .select('start_time','hour', 'day', 'week', 'month', 'year', 'weekday') \
                   .drop_duplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy('year', 'month').parquet(output_data + 'time_table/')
    
    # define schema to prevent spark inferring null types
    song_schema = StructType([
        StructField('artist_id', StringType(), False),
        StructField('artist_latitude', DoubleType(), True),
        StructField('artist_location', StringType(), True),
        StructField('artist_longitude', DoubleType(), True),
        StructField('artist_name', StringType(), True),
        StructField('duration', DoubleType(), True),
        StructField('num_songs', IntegerType(), True),
        StructField('song_id', StringType(), False),
        StructField('title', StringType(), False),
        StructField('year', IntegerType(), False)
    ])
    
    # read in song data to use for songplays table
    song_df = spark.sql(
        'SELECT DISTINCT song_id, title, artist_id, artist_name,duration FROM df_song_table'
    )
    
    # extract columns from joined song and log datasets to create songplays table
    songplays_table = df.join(song_df, (df.song == song_df.title) & (df.artist == song_df.artist_name) & (df.length == song_df.duration), 'left_outer') \
    .distinct() \
    .select(monotonically_increasing_id().alias("songplay_id"),
            col("start_time"),
            col("userId").alias("user_id"),
            col("level"),
            col("song_id"),
            col("artist_id"),
            col("sessionId").alias('session_id'),
            col("location"),
            col("userAgent").alias("user_agent"),
           ).withColumn("month", month(col("start_time"))) \
    .withColumn("year", year(col("start_time")))
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partitionBy('year', 'month').parquet(output_data + 'songplays_table/')

def main():
    """
    Orchestrates the program that will read data from an S3 bucket, perform ETL on it and write tables to a different S3 bucket.
    """
    spark = create_spark_session()
    input_data = 's3a://udacity-dend/'
    output_data = 's3a://spark-buckety/'
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == '__main__':
    main()
