# `pcw/clickhouser`
This repo provides scripts to initialize a Clickhouse container with retroactive PCW network data for querying and analysis, as well as provides an injest script to load network data added after initial setup. 

## Loading retroactive network data 

This uses built-in functionality of the `clickhouse` container to load retroactive network data, specifically 
running a setup script following db initialization that 
1) Creates a new database 
2) Creates tables for previous years by loading data directly from s3 

This requires mounting a volume that contains the setup script, which is 
contained in `./setup_scripts`

You will also need a `.env` file that contains 

**$schema goes here**

To set up the container, simply `docker compose up`. The container will load retroactive data, then restart. 


# Injesting future data

This script injests any new list_clients files for the current date (or an arbitrary date) from object storage. It can be run as many times per day as needed; if there is no new data it will simply exit.

## Installation
### Native using a virtual environment
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

(For Windows, replace `export` with `set` in your virtual environment)

Obviously, replace the password and any other values as needed.

Now activate the virtual environment:

```sh
source .venv/bin/activate
```

Install dependencies:

```
pip install -r requirements.txt
```

## Container 
* TODO - see `injest_dockerfile`
* `docker build . -t injest` / `docker run -it injest [db_connection.py, scheduled_injest.py]`

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

## Reporting

You can also run `python3 check_missing_paths.py` to get a report on any JSON
files currently in object storage that have not been injested. Adding the `--save`
switch will save the full list to a text file.

It may be advisable to run this periodically, to ensure that no files were missed.

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

# Querying the data 
To query the database, run a `clickhouse-client` container 
```docker
docker run -it --rm --network=container:pcw-clickhouse --entrypoint clickhouse-client clickhouse/clickhouse-server --user pcw
```

To run the server container on it's own and not part of a Compose stack: 
```docker
docker run -d --env-file .env -v "./setup_scripts/load_retroactive.sh:/docker-entrypoint-initdb.d --name some-clickhouse-server --ulimit nofile=262144:262144 clickhouse/clickhouse-server
```