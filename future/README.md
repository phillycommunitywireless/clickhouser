# ClickHouse PCW controller data injestion script.

This script injests any new list_clients files for the current date (or an
arbitrary date) from object storage. It can be run as many times per day as
needed; if there is no new data it will simply exit.

## Installation

Create a ClickHouse container with credentials for Linode object storage:

```sh
docker run -d -p 18123:8123 -p19000:9000 -e CLICKHOUSE_PASSWORD=YOUR+PASSWORD \
  -e CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1 \
  -e AWS_ACCESS_KEY_ID=YOUR_ACCESS_KEY \
  -e AWS_SECRET_ACCESS_KEY=YOUR_SECRET_ACCESS_KEY --name pcw-clickhouse \
  --ulimit nofile=262144:262144 clickhouse/clickhouse-server
```

Change all the values above as appropriate.

Note there are better ways to pass these credentials, such as an .env file.
Should update this doc later with that.

Now set up the python virtual environment:

```sh
python3 -m venv .venv
```

Add the following variables to your virtual environment. You can add the lines
directly to the file .venv/bin/activate that was created with the previous
command:

```sh
export AWS_URL="region.objectstorage.com"
export AWS_BUCKET="your-bucket-name"
export AWS_ACCESS_KEY_ID=ACCESS_KEY
export AWS_SECRET_ACCESS_KEY=SECRET_ACCESS_KEY
export CLICKHOUSE_HOST=localhost
export CLICKHOUSE_PORT=19000
export CLICKHOUSE_USER=default
export CLICKHOUSE_PASSWORD=password
export CLICKHOUSE_DB=default
```

Obviously, replace the password and any other values as needed.

Now activate the virtual environment:

```sh
source .venv/bin/activate
```

Install dependencies:

```
pip install -r requirements.txt
```

## Database setup

To create necessary database schemas, run:

```
python3 db_connection.py
```

## Usage

You can now injest the current day's data. Run:

```sh
python3 scheduled_injest.py
```

You should see output that looks something like:

```
Row count for list_clients: 11260
Row count for list_clients where _path contains "2025/06/05": 0
Rows to insert: 3488

Running query: INSERT INTO list_clients SELECT _path,* FROM s3('https://us-east-1.linodeobjects.com/your-bucket-name/2025/06/05/list_clients--**.json') WHERE _path NOT IN (SELECT DISTINCT _path FROM list_clients)

Query successful.
Row count for list_clients: 14748
Row count for list_clients where _path contains "2025/06/05": 3488
```

You can also supply a different date:

```sh
python3 scheduled_injest.py "2025/06/05"
```

## Logs

All injest events are logged in the injest_log table. To open a ClickHouse
console, run:

```
docker run -it --rm --network=container:pcw-clickhouse --entrypoint clickhouse-client clickhouse/clickhouse-server
```

To see the last log entry, try:


```sql
SELECT * FROM injest_log ORDER BY time DESC LIMIT 1 FORMAT Vertical
```