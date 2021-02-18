import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = f"{input_data}/song_data/*/*/*"

    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    song_cols = ["song_id", "title", "artist_id", "year", "duration"]
    songs_table = df.select(*song_cols)

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year","artist_id").parquet(f"{output_data}/songs")

    # extract columns to create artists table
    artist_cols = ["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]
    artists_table = df.select(*artist_cols)

    # write artists table to parquet files
    artists_table.write.parquet(f"{output_data}/artists")


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = f"{input_data}/log_data"

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.filter("auth == 'Logged In'")

    # extract columns for users table
    users_cols = [
        "userId as user_id",
        "firstName as first_name",
        "lastName as last_name",
        "gender",
        "level"]
    users_table = df.selectExpr(*users_cols)

    # write users table to parquet files
    users_table.write.parquet(f"{output_data}/users")

    # create datetime column from original timestamp column
    df = df.withColumn("start_time", from_unixtime(col('ts')/1000))

    # extract columns to create time table
    time_table = df.select('start_time') \
        .withColumn("hour", hour("start_time")) \
        .withColumn("day", dayofmonth("start_time")) \
        .withColumn("week", weekofyear("start_time")) \
        .withColumn("month", month("start_time")) \
        .withColumn("year", year("start_time")) \
        .withColumn("weekday", dayofweek("start_time"))

    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(f"{output_data}/time")

    # read in song data to use for songplays table
    song_data = f"{input_data}/song_data/*/*/*"
    song_df = spark.read.json(song_data)

    # extract columns from joined song and log datasets to create songplays table
    join_cond = [
        df.song == song_df.title,
        df.artist == song_df.artist_name,
        df.length == song_df.duration
    ]
    songplay_cols = [
        "start_time",
        "userId as user_id",
        "level",
        "song_id",
        "artist_id",
        "sessionId as session_id",
        "location",
        "userAgent as user_agent"
    ]
    songplays_table = df.join(
        song_df,
        join_cond,
        'inner'
    ).selectExpr(*songplay_cols)


    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(f"{output_data}/songplays")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
