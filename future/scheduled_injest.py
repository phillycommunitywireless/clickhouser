import db_connection
from datetime import datetime

def log_injest_event(s3_url, row_count, row_count_today, insert_count, insert_query=None, error=None, new_row_count=None, new_row_count_today=None):
    client = db_connection.get_db_client()
    current_time = datetime.now()
    client.execute(
        "INSERT INTO injest_log (time, s3_url, row_count, row_count_today, insert_count, insert_query, error, new_row_count, new_row_count_today) VALUES", 
        [(current_time, s3_url, row_count, row_count_today, insert_count, insert_query, error, new_row_count, new_row_count_today)]
    )

def injest():
    client = db_connection.get_db_client()

    # Get today's date in YYYY/MM/DD format
    today = datetime.now().strftime("%Y/%m/%d")

    # Query the database to get the current count of rows
    row_count = client.execute("SELECT count(*) FROM list_clients")[0][0]
    print(f"Current row count for list_clients: {row_count}")

    # Get a count of rows in list_clients whose _path contains today's date
    row_count_today = client.execute(f"SELECT count(*) FROM list_clients WHERE _path LIKE 'pcw-data-cron/{today}%'")[0][0]
    print(f"Row count for list_clients from objects from today's scrape ({today}): {row_count_today}")

    # Construct the S3 URL with today's date
    s3_url = f"https://us-east-1.linodeobjects.com/pcw-data-cron/{today}/list_clients--**.json"

    # Execute the INSERT query
    path_select = "SELECT DISTINCT _path FROM list_clients"
    count_query = f"SELECT count(*) FROM s3('{s3_url}') WHERE _path NOT IN ({path_select})"
    insert_count = client.execute(count_query)[0][0]
    print(f"Number of new rows to insert: {insert_count}")

    if (insert_count == 0):
        log_injest_event(s3_url, row_count, row_count_today, insert_count)
        exit()

    insert_query = f"INSERT INTO llist_clients SELECT _path,* FROM s3('{s3_url}') WHERE _path NOT IN ({path_select})"
    print()
    print(f"Running query: {insert_query}")

    try:
        result = client.execute(insert_query)
    except Exception as e:
        print(f"Error executing query: {str(e)}")
        log_injest_event(s3_url, row_count, row_count_today, insert_count, insert_query, str(e))
        exit()

    print(f"Successfully inserted data.")
    new_row_count = client.execute("SELECT count(*) FROM list_clients")[0][0]
    print(f"New row count for list_clients after insert: {new_row_count}")
    new_row_count_today = client.execute(f"SELECT count(*) FROM list_clients WHERE _path LIKE 'pcw-data-cron/{today}%'")[0][0]
    print(f"Row count for list_clients from objects from today's scrape after insert: {new_row_count_today}")
    log_injest_event(s3_url, row_count, row_count_today, insert_count, insert_query, None, new_row_count, new_row_count_today)

if __name__ == "__main__":
    injest()