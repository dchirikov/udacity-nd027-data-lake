# Project overview

The goal of the project it to design ETL processing pipeline based on Spark.
ETL loads song and log data from S3 bucket to Spark data frames,
then slit it onto 5 parquet files

# Dataframes

## songplays

Records in log data associated with song plays i.e. records with page NextSong

Columns:

songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent


## users

Users in the app

Columns:

user_id, first_name, last_name, gender, level

## songs

Songs in music database

Columns:

song_id, title, artist_id, year, duration

## artists

Artists in music database

Columns:

artist_id, name, location, lattitude, longitude

## time

Timestamps of records in songplays broken down into specific units

Columns:

start_time, hour, day, week, month, year, weekday

# Files of the project

- `README.md` - this file.
- `etl.py` - defiles ELT pipeline.
- `requirements.txt` requirements for linter checker
- `Makefile` - supports `make pep8` for checking PEP8 compliance
- `dl.cfg.template` template for `dl.cfg` file.
- `dl.cfg` - not part of the repo, but is required to run ELT (see `dl.cfg.template`)

# How to run

Please put `dl.cfg` and `etl.py` on the Spark master host, then run `spark-submit --master yarn etl.py`
