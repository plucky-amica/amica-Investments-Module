import sys
import boto3
import urllib.parse
import pandas as pd
from io import StringIO
import time
import awswrangler as wr
from datetime import datetime
import json
from pytz import timezone
import logging

# Setup logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS service clients
s3 = boto3.client("s3")
sns = boto3.client('sns')
glue = boto3.client("glue")
athena = boto3.client('athena')
lambda_client = boto3.client('lambda')

def sns_notification(error_type, message):
    """ Send notifications using SNS for different types of errors. """
    topic_arn = 'arn:aws:sns:us-east-2:392523246457:Failure_notification'
    sns.publish(TopicArn=topic_arn, Message=message)
    logger.error(f"Notification sent - {error_type}: {message}")

def process_file(file_content, filename):
    """ Process file content to parse, validate, and potentially transform data. """
    try:
        df = pd.read_csv(StringIO(file_content), skiprows=7, dtype=your_dtype_dict, skipfooter=1, engine='python')
        if df.empty:
            raise ValueError("Dataframe is empty")
        return df
    except Exception as e:
        sns_notification('critical', f'Error processing file {filename}: {str(e)}')
        return None

def athena_query_call(query):
    """ Execute an Athena query and handle response. """
    s3_save_loc = "s3://amica-aws-eds-dev-engineers-test/inv_export_output/pricing_dt_checker/"
    try:
        response = athena.start_query_execution(QueryString=query, QueryExecutionContext={"Database": "amica_eds_dev_engineers_test_db"}, ResultConfiguration={"OutputLocation": s3_save_loc})
        query_exec_id = response["QueryExecutionId"]
        logger.info(f"Query started: {query_exec_id}")

        # Check the query execution
        while True:
            response = athena.get_query_execution(QueryExecutionId=query_exec_id)
            if response['QueryExecution']['Status']['State'] in ["SUCCEEDED", "FAILED", "CANCELLED"]:
                break
            time.sleep(1)
        
        if response['QueryExecution']['Status']['State'] == "SUCCEEDED":
            result_object_key = f"{s3_save_loc.split('/')[-1]}{query_exec_id}.csv"
            result = wr.s3.read_csv(path=f"s3://{s3_save_loc.split('/')[2]}/{result_object_key}")
            wr.s3.delete_objects([result_object_key])
            return result
    except Exception as e:
        sns_notification('critical', f'Error executing Athena query: {str(e)}')
        return None

def process(input_bucket, input_key):
    """ Process the input S3 bucket based on provided key. """
    response_iterator = s3.get_paginator('list_objects_v2').paginate(Bucket=input_bucket, Prefix=input_key)
    for page in response_iterator:
        for obj in page['Contents']:
            file_key = obj['Key']
            content_object = s3.get_object(Bucket=input_bucket, Key=file_key)
            file_content = content_object['Body'].read().decode('utf-8')
            df = process_file(file_content, file_key)
            if df is not None:
                logger.info(f"File processed successfully: {file_key}")
                return df  # or some processing result
    return None

def lambda_handler(event, context):
    """ Main entry point for the Lambda function. """
    try:
        record = event['Records'][0]
        input_bucket = record['s3']['bucket']['name']
        input_key = urllib.parse.unquote_plus(record['s3']['object']['key'])
        logger.info(f"Processing file: {input_key} from bucket: {input_bucket}")
        
        processing_result = process(input_bucket, input_key)
        if processing_result:
            # Additional logic or Lambda invocation
            response = lambda_client.invoke(
                FunctionName='arn:aws:lambda:us-east-2:392523246457:function:rravichandran_test_fi_export',
                InvocationType='Event',
                Payload=json.dumps({"message": "Processing complete"})
            )
            logger.info("Further processing Lambda invoked.")
        else:
            logger.error("Processing failed or no valid data.")
    except Exception as e:
        sns_notification('critical', f'Lambda execution failed: {str(e)}')

