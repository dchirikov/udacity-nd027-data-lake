"""Extract-transform-load pipeline on Spark"""
import configparser
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col  # pylint: disable=no-name-in-module
from pyspark.sql.functions import (
    year,
    month,
    dayofmonth,
    hour,
    dayofweek,
    weekofyear,
    from_unixtime
)


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config.get(
    'default',
    'AWS_ACCESS_KEY_ID'
)
os.environ['AWS_SECRET_ACCESS_KEY'] = config.get(
    'default',
    'AWS_SECRET_ACCESS_KEY'
)


def create_spark_session():
    """Connect to Spark"""
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """Read song data and write into 'songs' and 'artists' files"""

    # get filepath to song data file
    song_data = f"{input_data}/song_data/*/*/*"
    # song_data = f"{input_data}/song_data/A/D/I"

    # read song data file
    song_df = spark.read.json(song_data)

    # extract columns to create songs table
    song_cols = ["song_id", "title", "artist_id", "year", "duration"]
    songs_table = song_df.select(*song_cols)

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id") \
        .parquet(f"{output_data}/songs")

    # extract columns to create artists table
    artist_cols = [
        "artist_id",
        "artist_name",
        "artist_location",
        "artist_latitude",
        "artist_longitude"
    ]
    artists_table = song_df.select(*artist_cols)

    # write artists table to parquet files
    artists_table.write.parquet(f"{output_data}/artists")


def process_log_data(spark, input_data, output_data):
    """Read log data and write into 'users', 'time' and 'songplays' files"""

    # get filepath to log data file
    log_data = f"{input_data}/log_data/*/*"

    # read log data file
    log_df = spark.read.json(log_data)

    # filter by actions for song plays
    log_df = log_df.filter("auth == 'Logged In'")

    # extract columns for users table
    users_cols = [
        "userId as user_id",
        "firstName as first_name",
        "lastName as last_name",
        "gender",
        "level"]
    users_table = log_df.selectExpr(*users_cols)

    # write users table to parquet files
    users_table.write.parquet(f"{output_data}/users")

    # create datetime column from original timestamp column
    log_df = log_df.withColumn("start_time", from_unixtime(col('ts')/1000))

    # extract columns to create time table
    time_table = log_df.select('start_time') \
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
    # song_data = f"{input_data}/song_data/A/D/I"
    song_df = spark.read.json(song_data)

    # extract columns from joined song and log
    # datasets to create songplays table
    join_cond = [
        log_df.song == song_df.title,
        log_df.artist == song_df.artist_name,
        log_df.length == song_df.duration
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
    songplays_table = log_df.join(
        song_df,
        join_cond,
        'inner'
    ).selectExpr(*songplay_cols)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(f"{output_data}/songplays")


def main(conf):
    """Main entrypoint"""

    spark = create_spark_session()

    input_data = conf.get("s3", "input")
    output_data = conf.get("s3", "output")

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main(config)
