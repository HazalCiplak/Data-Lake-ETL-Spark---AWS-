import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg',encoding='utf-8-sig')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']



def create_spark_session():
    spark = SparkSession.builder.config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0").getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Load data from s...
    ----------
    spark:This is the spark session that has been created
    input_data: This is the path to the song_data s3 bucket.
    output_data: This is the path to where the parquet files will be written.
    """
    
    # getting filepath to song data file
    song_data = input_data+'song_data/A/A/*/*.json'
    # reading song data file
    df = spark.read.json(song_data)
    
    # extracting columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration').dropDuplicates()
    songs_table.createOrReplaceTempView('songs')
    
    # writing songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(path = output_data+'/songs/songs.parquet', mode = 'overwrite')
    
    # extracting columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location','artist_latitude', 'artist_longitude').dropDuplicates()
    artists_table.createOrReplaceTempView('artists')
    
    # writing artists table to parquet files
    artists_table.write.parquet(path = output_data + '/artists/artists.parquet', mode = 'overwrite')
    
    
def process_log_data(spark, input_data, output_data):
    """
    Load data from s...
    ----------
    spark:This is the spark session that has been created
    input_data: This is the path to the song_data s3 bucket.
    output_data: This is the path to where the parquet files will be written.
    """
    
    # getting filepath to log data file
    log_data = input_data + 'log_data/*.json'
    
    # reading log data file
    df = spark.read.json(log_data)
    
    # filtering by actions for song plays
    df = df.filter(df.page == 'NextSong').select('ts', 'userId', 'level', 'song', 'artist','sessionId', 'location', 'userAgent')
    
    # extracting columns for users table    
    users_table = df.select('userId', 'firstName', 'lastName','gender', 'level').dropDuplicates()
    users_table.createOrReplaceTempView('users')
    
    # writing users table to parquet files
    users_table.write.parquet(path = output_data + 'users/users.parquet', mode = 'overwrite')
    
    # creating timestamp column from original timestamp column
    get_datetime = udf(lambda x: str(int(int(x)/1000)))
    df = df.withColumn('timestamp', get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000.0)))
    df = df.withColumn("datetime", get_datetime(df.ts))
    
    # extract columns to create time table
    time_table = df.select(
            col('datetime').alias('start_time'),
            hour('datetime').alias('hour'),
            dayofmonth('datetime').alias('day'),
            weekofyear('datetime').alias('week'),
            month('datetime').alias('month'),
            year('datetime').alias('year'),
            dayofweek('datetime').alias('weekday')
            ).dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(path = output_data + 'time/time.parquet', mode = 'overwrite')
    
    # read in song data to use for songplays table
    song_df = spark.read.json(input_data + 'song_data/A/A/A/*.json')
    
    
    # extract columns from joined song and log datasets to create songplays table 
    
    df_joined = df.join(song_df, song_df.title == df.song,'inner')
    
    songplays_table = df_joined.select(
        col('ts').alias('start_time'),
        col('userId').alias('user_id'),
        col('level').alias('level'),
        col('song_id').alias('song_id'),
        col('artist_id').alias('artist_id'),
        col('ssessionId').alias('session_id'),
        col('location').alias('location'),
        col('userAgent').alias('user_agent'),
        col('year').alias('year'),
        month('datetime').alias('month')
        ).dropDuplicates()
    
    
    songplays_table.select(monotonically_increasing_id().alias('songplay_id')).collect()
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month').parquet(path = output_data + 'songplays.parquet', mode = 'overwrite')
    
    
def main():                  
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://hazaltestbucket2us/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)
    
    
if __name__ == "__main__":
    main()
