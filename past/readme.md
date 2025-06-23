# Loading retroactive network data 

This uses built-in functionality of the `clickhouse` container to load retroactive network data, specifically 
running a setup script following db initialization that 
1) Creates a new database called "pcw" 
2) Creates tables for 2024 and 2025 via loading data directly from s3. 

This requires mounting a volume that contains the setup script, which is 
contained in `./setup_scripts`

You will also need a `.env` file that contains 
* The URL for the required buckets 
* An access/secret key from s3 

The .env file should look like this: 
* S3_ACCESS_KEY 
* S3_SECRET_KEY
* DATA_URL_2024 
* DATA_URL_2025  

To run the container with the mounted volume, run the following command from the `past` directory: 
`docker run -d --env-file .env -v "%cd%/setup_scripts":/docker-entrypoint-initdb.d --name some-clickhouse-server --ulimit nofile=262144:262144 clickhouse/clickhouse-server`

To run the clickhouse client: 
`docker run -it --rm --network=container:some-clickhouse-server --entrypoint clickhouse-client clickhouse/clickhouse-server`

Once in the client, check 1) that the new db has been created and 2) the tables have been created with `show databases` and `show tables from pcw`. 

