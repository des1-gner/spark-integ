import boto3
import pandas as pd
from datetime import datetime
import random

# 1. Create S3 client
s3_client = boto3.client('s3')

# 2. Create a unique bucket name (S3 bucket names must be globally unique)
bucket_name = f"test-parquet-bucket-{datetime.now().strftime('%Y%m%d%H%M%S')}"
region = 'us-east-1'  # Change to your preferred region

print(f"Creating bucket: {bucket_name}")

try:
    # Create the bucket
    if region == 'us-east-1':
        s3_client.create_bucket(Bucket=bucket_name)
    else:
        s3_client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={'LocationConstraint': region}
        )
    print(f"Bucket {bucket_name} created successfully")
    
    # 3. Create mock data and upload as parquet files
    months = ["202501", "202502", "202503", "202504"]
    
    for month in months:
        # Create mock data
        data = {
            'user_id': [f'user_{i}' for i in range(1000)],
            'action': [random.choice(['click', 'view', 'purchase', 'login']) for _ in range(1000)],
            'timestamp': [datetime.now().strftime('%Y-%m-%d %H:%M:%S') for _ in range(1000)],
            'month': [month] * 1000
        }
        
        # Create DataFrame and save as parquet
        df_pandas = pd.DataFrame(data)
        parquet_buffer = df_pandas.to_parquet()
        
        # Upload to S3
        key = f"user_action_month_{month}.parquet"
        s3_client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=parquet_buffer
        )
        print(f"Uploaded {key}")
    
    # 4. Now read the files with Spark
    base_path = f"s3://{bucket_name}/"
    paths = [f"{base_path}user_action_month_{m}.parquet" for m in months]
    
    print("Reading parquet files with Spark...")
    df = spark.read.parquet(*paths)
    
    print("Data loaded successfully!")
    df.show(10)
    print(f"Total rows: {df.count()}")
    
except Exception as e:
    print(f"Error: {e}")
