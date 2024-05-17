import boto3
import time
import schedule
import logging
from datetime import datetime
from dotenv import load_dotenv
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Load environment variables from .env file
load_dotenv()

# Access keys and region from environment variables
aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
aws_region = os.getenv('AWS_REGION')

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Initialize boto3 clients
datasync = boto3.client('datasync', region_name=aws_region,
                        aws_access_key_id=aws_access_key_id,
                        aws_secret_access_key=aws_secret_access_key)
cloudwatch = boto3.client('cloudwatch', region_name=aws_region,
                          aws_access_key_id=aws_access_key_id,
                          aws_secret_access_key=aws_secret_access_key)

# Configuration parameters
agent_arn = 'YOUR_DATASYNC_AGENT_ARN'
source_location_uri = 'YOUR_ON_PREMISES_SOURCE_URI'
s3_bucket_name = 'your-s3-bucket-name'
destination_location_uri = f's3://{s3_bucket_name}/your-target-prefix/'
iam_role_arn = 'YOUR_IAM_ROLE_ARN'
input_path = f's3a://{s3_bucket_name}/your-target-prefix/'
output_path = f's3a://{s3_bucket_name}/your-output-prefix/'

def create_location_on_premises():
    response = datasync.create_location_nfs(
        ServerHostname='YOUR_ON_PREMISES_SERVER_HOSTNAME',
        Subdirectory=source_location_uri,
        OnPremConfig={
            'AgentArns': [agent_arn]
        }
    )
    return response['LocationArn']

def create_location_s3():
    response = datasync.create_location_s3(
        S3BucketArn=f'arn:aws:s3:::{s3_bucket_name}',
        S3Config={
            'BucketAccessRoleArn': iam_role_arn
        }
    )
    return response['LocationArn']

def create_task(source_location_arn, destination_location_arn):
    response = datasync.create_task(
        SourceLocationArn=source_location_arn,
        DestinationLocationArn=destination_location_arn,
        Name='OnPremToS3Task'
    )
    return response['TaskArn']

def start_task_execution(task_arn):
    response = datasync.start_task_execution(
        TaskArn=task_arn
    )
    return response['TaskExecutionArn']

def monitor_task(task_execution_arn):
    while True:
        response = datasync.describe_task_execution(
            TaskExecutionArn=task_execution_arn
        )
        status = response['Status']
        logging.info(f'Task status: {status}')
        
        # Send custom metric to CloudWatch
        cloudwatch.put_metric_data(
            Namespace='DataSyncMetrics',
            MetricData=[
                {
                    'MetricName': 'TaskStatus',
                    'Dimensions': [
                        {
                            'Name': 'TaskExecutionArn',
                            'Value': task_execution_arn
                        },
                    ],
                    'Value': 1 if status == 'SUCCESS' else 0,
                    'Unit': 'Count',
                    'Timestamp': datetime.utcnow()
                },
            ]
        )
        
        if status in ['SUCCESS', 'ERROR']:
            break
        time.sleep(10)  # Wait for 10 seconds before checking the status again

def sync_data():
    logging.info("Starting data synchronization process")
    
    # Create locations if they do not exist (this can be skipped if locations are pre-created)
    source_location_arn = create_location_on_premises()
    destination_location_arn = create_location_s3()
    
    # Create and start the task
    task_arn = create_task(source_location_arn, destination_location_arn)
    task_execution_arn = start_task_execution(task_arn)
    
    # Monitor task
    monitor_task(task_execution_arn)
    
    logging.info("Data synchronization process completed")

    # Start PySpark session
    spark = SparkSession.builder \
        .appName("DataSyncPySpark") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.0") \
        .getOrCreate()
    
    # Read CSV files from S3
    df = spark.read.csv(input_path, header=True)

    # Perform transformations (example transformation)
    filtered_df = df.filter(col("city").isNotNull())

    # Show the filtered DataFrame
    filtered_df.show()

    # Write transformed data back to S3
    filtered_df.write.csv(output_path, mode="overwrite", header=True)

    # Stop SparkSession
    spark.stop()

# Schedule the sync_data function to run daily at 2 AM
schedule.every().day.at("02:00").do(sync_data)

# Keep the script running to maintain the schedule
while True:
    schedule.run_pending()
    time.sleep(1)
