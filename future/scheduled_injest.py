import db_connection
from datetime import datetime
import sys
import os

def log_injest_event(s3_url, row_count, row_count_today, insert_count, insert_query=None, error=None, new_row_count=None, new_row_count_today=None):
    client = db_connection.get_db_client()
    current_time = datetime.now()
    client.execute(
        "INSERT INTO injest_log (time, s3_url, row_count, row_count_today, insert_count, insert_query, error, new_row_count, new_row_count_today) VALUES", 
        [(current_time, s3_url, row_count, row_count_today, insert_count, insert_query, error, new_row_count, new_row_count_today)]
    )

def get_aws_url():
    if 'AWS_URL' in os.environ:
        aws_url = os.environ['AWS_URL']
    else:
        print("Error: AWS_URL environment variable is not set.")
        sys.exit(1)
    
    return aws_url

def get_date_prefix():
    if len(sys.argv) > 1:
        # Use command line argument if provided
        date_prefix = sys.argv[1]
        try:
            datetime.strptime(date_prefix, "%Y/%m/%d")  # Validate date format
        except ValueError:
            print(f"Invalid date format: {date_prefix}. Expected format is YYYY/MM/DD.")
            sys.exit(1)
    else:
        # Otherwise use today's date
        date_prefix = datetime.now().strftime("%Y/%m/%d")
    return date_prefix


def injest(aws_url, date_prefix):
    client = db_connection.get_db_client()

    # Query the database to get the current count of rows
    row_count = client.execute("SELECT count(*) FROM list_clients")[0][0]
    print(f"Row count for list_clients: {row_count}")

    # Get a count of rows in list_clients whose _path contains today's date
    row_count_today = client.execute(f"SELECT count(*) FROM list_clients WHERE _path LIKE 'pcw-data-cron/{date_prefix}%'")[0][0]
    print(f"Row count for list_clients where _path contains \"{date_prefix}\" {row_count_today}")

    # Construct the S3 URL with today's date
    s3_url = f"{aws_url}/{date_prefix}/list_clients--**.json"

    # Execute the INSERT query
    path_select = "SELECT DISTINCT _path FROM list_clients"
    count_query = f"SELECT count(*) FROM s3('{s3_url}') WHERE _path NOT IN ({path_select})"
    insert_count = client.execute(count_query)[0][0]
    print(f"Rows to insert: {insert_count}")

    if (insert_count == 0):
        log_injest_event(s3_url, row_count, row_count_today, insert_count)
        sys.exit(0)

    insert_query = f"INSERT INTO list_clients SELECT _path,* FROM s3('{s3_url}') WHERE _path NOT IN ({path_select})"
    print()
    print(f"Running query: {insert_query}")
    print()

    try:
        result = client.execute(insert_query)
    except Exception as e:
        print(f"Error executing query: {str(e)}")
        log_injest_event(s3_url, row_count, row_count_today, insert_count, insert_query, str(e))
        sys.exit(1)

    print(f"Query successful.")
    new_row_count = client.execute("SELECT count(*) FROM list_clients")[0][0]
    print(f"Row count for list_clients: {new_row_count}")
    new_row_count_today = client.execute(f"SELECT count(*) FROM list_clients WHERE _path LIKE 'pcw-data-cron/{date_prefix}%'")[0][0]
    print(f"Row count for list_clients where _path contains \"{date_prefix}\": {new_row_count_today}")
    log_injest_event(s3_url, row_count, row_count_today, insert_count, insert_query, None, new_row_count, new_row_count_today)

if __name__ == "__main__":
    aws_url = get_aws_url()
    date_prefix = get_date_prefix()
    db_connection.validate_database()
    injest(aws_url, date_prefix)