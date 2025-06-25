#!/bin/bash
set -e

clickhouse client -n <<-EOSQL
    # allow nullable columns to be PKs 
    set schema_inference_make_columns_nullable=0;

    # allow PK to be nullable 
    # set allow_nullable_key = 1 

    # create db  
    create database pcw;

    # -------------------------------------------------------------------------------------------------------- #

    # Create tables for calendar years 
    # 2024
    # create table for 2024 
    # create table pcw.2024_list_clients order by _id as select * from s3($DATA_URL_2024, $S3_ACCESS_KEY, $S3_SECRET_KEY);

    # 2025 
    # create table for 2025 
    # create table pcw.2025_list_clients order by _id as select * from s3($DATA_URL_2025);

    # -------------------------------------------------------------------------------------------------------- #

    # Create tables for fiscal years 
    # FY2023 
    create table pcw.fy2023 order by _id settings allow_nullable_key = 1 as select _id, mac from s3($DATA_URL_FY23_1, $S3_ACCESS_KEY, $S3_SECRET_KEY);
    insert into pcw.fy2023 select _id, mac from s3($DATA_URL_FY23_2, $S3_ACCESS_KEY, $S3_SECRET_KEY);

    # FY2024 
    # only get _id and mac due to problematic decimals in some fields 
    create table pcw.fy2024 order by _id settings allow_nullable_key = 1 as select _id, mac from s3($DATA_URL_FY24_1, $S3_ACCESS_KEY, $S3_SECRET_KEY);
    insert into pcw.fy2024 select _id, mac from s3($DATA_URL_FY24_2, $S3_ACCESS_KEY, $S3_SECRET_KEY);

    # FY2025 
    create table pcw.fy2025 order by _id as select * from s3($DATA_URL_FY25_1, $S3_ACCESS_KEY, $S3_SECRET_KEY);
    insert into pcw.fy2025 (*) select * from s3($DATA_URL_FY25_2, $S3_ACCESS_KEY, $S3_SECRET_KEY);

    # -------------------------------------------------------------------------------------------------------- #

    # End of inserts 
    show tables in pcw; 

    # query the tables for unique MAC addresses and total MAC addresses  
    select count(distinct mac), count(mac) from pcw.fy2023 ; select count(distinct mac), count(mac) from pcw.fy2024 ; select count(distinct mac), count(mac) from pcw.fy2025;
    
EOSQL 

