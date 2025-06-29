import os
from clickhouse_driver import Client

def get_db_client():
    host = os.getenv("CLICKHOUSE_HOST", "localhost")
    port = int(os.getenv("CLICKHOUSE_PORT", 9000))
    user = os.getenv("CLICKHOUSE_USER", "default")
    password = os.getenv("CLICKHOUSE_PASSWORD", "")
    database = os.getenv("CLICKHOUSE_DB", "pcw_clickhouse")

    return Client(host=host, port=port, user=user, password=password, database=database)

def initialize_database():
    client = get_db_client()

    # Create the list_clients table if it doesn't exist
    client.execute('''
        CREATE TABLE IF NOT EXISTS list_clients (
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
            `bytes-r` Float64,
            `tx_bytes-r` Float64,
            `rx_bytes-r` Float64,
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
            `wired-tx_bytes` Int64 NULL,
            `wired-rx_bytes` Int64 NULL,
            `wired-tx_packets` Int64 NULL,
            `wired-rx_packets` Int64 NULL,
            `wired-tx_bytes-r` Int64 NULL,
            `wired-rx_bytes-r` Int64 NULL,
            detailed_states Tuple (uplink_near_power_limit Bool)
        )
        ENGINE = MergeTree()
        ORDER BY _id
    ''')

    # Create the injest_log table if it doesn't exist
    client.execute('''
        CREATE TABLE IF NOT EXISTS injest_log (
            time DateTime DEFAULT now(),
            s3_url String,
            row_count Int64,
            row_count_today Int64,
            insert_count Int64,
            insert_query String NULL,
            error String NULL,
            new_row_count Int64 NULL,
            new_row_count_today Int64 NULL
        ) ENGINE = Log
    ''')

def validate_database():
    client = get_db_client()
    tables_to_check = ['list_clients', 'injest_log']
    for table in tables_to_check:
        try:
            client.execute(f"SELECT count(*) FROM {table} LIMIT 1")
        except Exception:
            print(f"Error: {table} table not found. Run the db_connection script and try again.")
            exit(1)

if __name__ == "__main__":
    initialize_database()
