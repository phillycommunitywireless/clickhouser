import db_connection
import boto3
from datetime import datetime
import sys
import os
import re
from urllib.parse import urlparse

SAMPLE_LIST_LENGTH: int = 25  # Number of missing paths to display in console output

def get_aws_info():
    """Get AWS credentials from environment variables"""
    aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    aws_url = os.environ['AWS_URL']
    aws_bucket = os.getenv("AWS_BUCKET", "pcw-data-cron")
    
    if not aws_access_key_id or not aws_secret_access_key or not aws_url:
        print("Error: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_URL and AWS_BUCKJET environment variables must be set.")
        sys.exit(1)
    
    return aws_access_key_id, aws_secret_access_key, aws_url, aws_bucket

def get_s3_paths(aws_access_key_id, aws_secret_access_key, aws_url, aws_bucket):
    """
    Get all S3 keys that follow the pattern '/20??/??/??/list-clients--*'
    using boto3 with paginator
    """
    boto3_url = "https://{}".format(aws_url)
    # Create S3 client
    s3_client = boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        endpoint_url=boto3_url
    )
    
    # Pattern to match /20??/??/??/list_clients--*
    pattern = re.compile(r'20\d{2}/\d{2}/\d{2}/list_clients--.*\.json$')
    
    s3_paths = []
    
    try:
        # Use paginator to handle large numbers of objects
        paginator = s3_client.get_paginator('list_objects_v2')
        
        page_iterator = paginator.paginate(Bucket=aws_bucket)
        for page in page_iterator:
            if 'Contents' in page:
                for obj in page['Contents']:
                    key = obj['Key']
                    if pattern.match(key):
                        # Convert S3 key to _path format (remove base prefix if present)
                        path = key
                        # Add bucket name to beginning of the path to match Clickhouse
                        path = f"{aws_bucket}/{path}"
                        s3_paths.append(path)
        
        print(f"Found {len(s3_paths)} files matching pattern in S3")
        return s3_paths
        
    except Exception as e:
        print(f"Error querying object storage: {str(e)}")
        sys.exit(1)

def get_db_paths():
    """Get all unique _path values from the list_clients table"""
    client = db_connection.get_db_client()
    
    try:
        query = "SELECT DISTINCT _path FROM list_clients ORDER BY _path"
        result = client.execute(query)
        db_paths = [row[0] for row in result]
        print(f"Found {len(db_paths)} unique paths in database")
        return db_paths
    except Exception as e:
        print(f"Error querying database: {str(e)}")
        sys.exit(1)

def find_missing_paths(s3_paths, db_paths):
    """Find paths that are present in S3 but not in the database"""
    db_paths_set = set(db_paths)
    missing_paths = [path for path in s3_paths if path not in db_paths_set]
    return missing_paths

def save_to_file(missing_paths, filename=None):
    """Save missing paths to a file"""
    if filename is None:
        filename = f"missing_paths_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    
    with open(filename, 'w') as f:
        for path in missing_paths:
            f.write(f"{path}\n")
    
    print(f"Full list saved to: {filename}")
    return filename

def main():
    print("Checking for missing paths between object storage and database...")
    print("=" * 60)
    
    aws_access_key_id, aws_secret_access_key, aws_url, aws_bucket = get_aws_info()
    db_connection.validate_database()
    
    db_paths = get_db_paths()
    s3_paths = get_s3_paths(aws_access_key_id, aws_secret_access_key, aws_url, aws_bucket)
    
    missing_paths = find_missing_paths(s3_paths, db_paths)
    
    print("=" * 60)
    print(f"Analysis complete:")
    print(f"  Object storage paths: {len(s3_paths)}")
    print(f"  Database paths: {len(db_paths)}")
    print(f"  Missing from database: {len(missing_paths)}")
    print("=" * 60)
    
    if missing_paths:
        print(f"\nFound {len(missing_paths)} paths present in object storage but missing from database:")
        print("-" * SAMPLE_LIST_LENGTH)
        
        # Limit console output for large lists
        if len(missing_paths) > SAMPLE_LIST_LENGTH:
            print(f"(Showing first {SAMPLE_LIST_LENGTH} paths - use --save to save full list to file)")
            for path in missing_paths[:SAMPLE_LIST_LENGTH]:
                print(f"  {path}")
            print(f"  ... and {len(missing_paths) - SAMPLE_LIST_LENGTH} more")
            
            # Check if --save flag was provided
            if "--save" in sys.argv:
                save_to_file(missing_paths)
        else:
            for path in missing_paths:
                print(f"  {path}")
    else:
        print("\nAll object storage paths are present in the database.")
    
    return missing_paths

if __name__ == "__main__":
    missing_paths = main()
    # Exit with code 1 if there are missing paths, 0 if all are present
    sys.exit(1 if missing_paths else 0)
