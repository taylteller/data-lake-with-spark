# Data Lake with Spark and AWS

## Introduction

A fictional music streaming startup, Sparkify, have a growing user base and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

The goal of this project is to build an ETL pipeline that extracts the data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow the analytics team to continue finding insights in what songs their users are listening to.

This project is part of the Udacity Data Engineering nanodegree.

## Databse Schema Design

Data is transferred into a star schema (one fact table where each row references data from multiple dimension tables). This schema design facilitates analytical querying, as it denormalizes data according to the particular aspects of the data we want to dig further into. In other words, it's optimized to minimize the number of JOINs required to query the data. 

### Fact Table

**songplays** - records in log data associated with song plays i.e. records with page NextSong
- songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

### Dimension Tables

**users** - users in the app
- user_id, first_name, last_name, gender, level

**songs** - songs in music database
- song_id, title, artist_id, year, duration

**artists** - artists in music database
- artist_id, name, location, latitude, longitude

**time** - timestamps of records in songplays broken down into specific units
- start_time, hour, day, week, month, year, weekday

## Project Structure

The folders and files in this repository are as follows:

- **README.md:** This explanatory Readme file
- **etl.py:** Script that reads the data from the S3 bucket, performs ETL on it, and writes it to another S3 bucket
- **dl.cfg:** Config file that provides credentials to connect to AWS. 

## How to Run

In order to run this project, a Redshift cluster on AWS is required. 

After cloning the repository and navigating to the root folder:

1. Include your AWS credentials in the `dl.cfg` file.

2. Change the output location for the data to an S3 bucket you can access.

3. Run the ETL pipeline
```
python etl.py
```
You are then free to explore the output files in your bucket.
