#!/bin/bash
set -e

echo "loading retroactive data..."

clickhouse client -n <<-EOSQL
    # allow nullable columns to be PKs 
    set schema_inference_make_columns_nullable=0;

    # allow PK to be nullable 
    # set allow_nullable_key = 1 

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
    # create table pcw.fy2023 order by _id settings allow_nullable_key = 1 as select _id, mac from s3($DATA_URL_FY23_1, $S3_ACCESS_KEY, $S3_SECRET_KEY);
    # insert into pcw.fy2023 select _id, mac from s3($DATA_URL_FY23_2, $S3_ACCESS_KEY, $S3_SECRET_KEY);

    # FY2024 
    # only get _id and mac due to problematic decimals in some fields 
    # create table pcw.fy2024 order by _id settings allow_nullable_key = 1 as select _id, mac from s3($DATA_URL_FY24_1, $S3_ACCESS_KEY, $S3_SECRET_KEY);
    # insert into pcw.fy2024 select _id, mac from s3($DATA_URL_FY24_2, $S3_ACCESS_KEY, $S3_SECRET_KEY);

    # FY2025 
    # create table pcw.fy2025 order by _id as select * from s3($DATA_URL_FY25_1, $S3_ACCESS_KEY, $S3_SECRET_KEY);
    # insert into pcw.fy2025 (*) select * from s3($DATA_URL_FY25_2, $S3_ACCESS_KEY, $S3_SECRET_KEY);

    # -------------------------------------------------------------------------------------------------------- #

    # End of inserts 
    # show tables in pcw; 

    # query the tables for unique MAC addresses and total MAC addresses  
    # select count(distinct mac), count(mac) from pcw.fy2023 ; select count(distinct mac), count(mac) from pcw.fy2024 ; select count(distinct mac), count(mac) from pcw.fy2025;

    # -------------------------------------------------------------------------------------------------------- #
    # -------------------------------------------------------------------------------------------------------- #

    CREATE DATABASE IF NOT EXISTS pcw_clickhouse;

    # list_clients
    # create table w/ schema 
    CREATE TABLE IF NOT EXISTS pcw_clickhouse.list_clients (
        _path String,
        site_id String,
        ap_mac String,
        assoc_time DateTime,
        latest_assoc_time DateTime,
        oui String,
        user_id String,
        last_ip String,
        last_uplink_name String,
        first_seen DateTime,
        last_seen DateTime,
        is_guest Bool,
        disconnect_timestamp DateTime,
        last_radio String,
        is_wired Bool,
        usergroup_id String,
        last_uplink_mac String,
        last_connection_network_name String,
        mac String,
        last_connection_network_id String,
        _id String,
        wlanconf_id String,
        _uptime_by_uap Int64,
        _last_seen_by_uap Int64,
        _is_guest_by_uap Bool,
        channel Int64,
        radio String,
        radio_name String,
        essid String,
        bssid String,
        powersave_enabled Bool,
        is_11r Bool,
        user_group_id_computed String,
        anomalies Int64,
        anon_client_id String,
        ccq Int64,
        dhcpend_time Int64,
        idletime Int64,
        noise Int64,
        nss Int64,
        rx_rate Int64,
        rssi Int64,
        satisfaction_now Int64,
        satisfaction_real Int64,
        satisfaction_reason Int64,
        signal Int64,
        tx_mcs Int64,
        tx_power Int64,
        tx_rate Int64,
        tx_retry_burst_count Int64,
        satisfaction Int64,
        is_mlo Bool,
        radio_proto String,
        channel_width Int64,
        satisfaction_avg Tuple (count Int64, total Int64),
        uptime Int64,
        tx_bytes Int64,
        rx_bytes Int64,
        tx_packets Int64,
        rx_packets Int64,
        
        # how to deal with these problematically named fields? 
        \`bytes-r\` Float64,
        \`tx_bytes-r\` Float64,
        \`rx_bytes-r\` Float64,

        tx_retries Int64,
        wifi_tx_attempts Int64,
        wifi_tx_dropped Int64,
        wifi_tx_retries_percentage Float64,
        authorized Bool,
        qos_policy_applied Bool,
        _uptime_by_usw Int64 NULL,
        _last_seen_by_usw Int64 NULL,
        _is_guest_by_usw Bool NULL,
        sw_mac String NULL,
        sw_port Int64 NULL,
        wired_rate_mbps Int64 NULL,
        sw_depth Int64 NULL,
        network String NULL,
        network_id String NULL,
        eagerly_discovered Bool NULL,
        hostname String NULL,
        ip String NULL,
        hostname_source String NULL,
        roam_count Int64 NULL,
        local_dns_record_enabled Bool NULL,
        virtual_network_override_id String NULL,
        use_fixedip Bool NULL,
        local_dns_record String NULL,
        virtual_network_override_enabled Bool NULL,
        noted Bool NULL,
        fixed_ap_enabled Bool NULL,
        name String NULL,
        fixed_ip String NULL,

        # how to deal with these problematically named fields? 

        \`wired-tx_bytes\` Int64 NULL,
        \`wired-rx_bytes\` Int64 NULL,
        \`wired-tx_packets\` Int64 NULL,
        \`wired-rx_packets\` Int64 NULL,
        \`wired-tx_bytes-r\` Int64 NULL,
        \`wired-rx_bytes-r\` Int64 NULL,
        detailed_states Tuple (uplink_near_power_limit Bool)
    )
    ENGINE = MergeTree()
    ORDER BY _id;

    # insert retroactive records
    insert into pcw_clickhouse.list_clients (*) select * from s3('$DATA_URL_FY25_2', '$S3_ACCESS_KEY', '$S3_SECRET_KEY');

EOSQL

echo "done loading retroactive data!"