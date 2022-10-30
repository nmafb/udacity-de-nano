# Project : Data Warehouse

## Introduction
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, i was tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights into what songs their users are listening to. 

## Project Description
In this project, i'll apply what i've learned on data warehouses and AWS to build an ETL pipeline for a database hosted on Redshift. To complete the project, i will need to load data from S3 to staging tables on Redshift and execute SQL statements that create the analytics tables from these staging tables.

## Datasets
* Song data: <code>s3://udacity-dend/song_data</code>
* Log data: <code>s3://udacity-dend/log_data</code>
* Log data json path: <code>s3://udacity-dend/log_json_path.json</code>
