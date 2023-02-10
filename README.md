# Project: Data Lake

## Project Description

This Project will allow Sparkify Startup analyze the data they've been collecting on songs and user activity on their new music streaming app. They will be able to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

The Project carried out several tasks including the creation of an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.


***

## Scripts execution with python from the project's root

### Execute ETL

    py etl.py

***

## Files in repository

1. **etl.py** reads data from S3, processes that data using Spark, and writes them back to S3.
2. **dl.cfg** contains the AWS credentials.


