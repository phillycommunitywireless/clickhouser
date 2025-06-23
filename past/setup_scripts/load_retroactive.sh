#!/bin/bash
set -e

clickhouse client -n <<-EOSQL
    # allow nullable columns to be PKs 
    set schema_inference_make_columns_nullable=0;

    # create db  
    create database pcw;

    # create table for 2024 from pcw-data
    create table pcw.2024_list_clients order by _id as select * from s3($DATA_URL_2024, $S3_ACCESS_KEY, $S3_SECRET_KEY);

    # create table for 2025 so far from pcw-cron 
    create table pcw.2025_list_clients order by _id as select * from s3($DATA_URL_2025, $S3_ACCESS_KEY, $S3_SECRET_KEY);
EOSQL 

