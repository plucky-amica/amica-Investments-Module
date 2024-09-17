import sys
import boto3
import urllib.parse
import time
from datetime import datetime
from pytz import timezone
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructType, StructField, DoubleType
import pandas as pd

# Initialize Spark and Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
s3 = boto3.client('s3')
glue = boto3.client("glue")
athena = boto3.client("athena")

# Function to trigger Glue Crawler
# Function to trigger Glue Crawler with status check
def crawler_trigger(crawler_name):
    try:
        # Check if the crawler is already running
        crawler_status = glue.get_crawler(Name=crawler_name)['Crawler']['State']
        
        if crawler_status == 'RUNNING':
            print(f"Crawler {crawler_name} is already running. Skipping trigger.")
        else:
            glue.start_crawler(Name=crawler_name)
            print(f"Successfully triggered Glue crawler: {crawler_name}")
    except Exception as e:
        print(f"Error checking or starting crawler: {e}")
        raise e


# Athena query function
def athena_query_call(query):
    s3_save_loc = "s3://amica-a035527-useast2-392523246457-dev/amica-a039986-prahalad-investments/Athena_output/inv_export_output/pricing_dt_checker/"
    
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": "inv_a039986_test"},
        ResultConfiguration={"OutputLocation": s3_save_loc}
    )
    
    query_exec_id = response["QueryExecutionId"]

    while True:
        response = athena.get_query_execution(QueryExecutionId=query_exec_id)
        status = response["QueryExecution"]["Status"]["State"]
        if status in ["SUCCEEDED", "FAILED", "CANCELLED"]:
            break
        time.sleep(1)

    if status == "SUCCEEDED":
        bucket = s3_save_loc.split('/')[2]
        key = "/".join(s3_save_loc.split('/')[3:]) + query_exec_id + ".csv"
        s3_path = f"s3://{bucket}/{key}"
        
        try:
            # Read Athena output using pandas
            df = pd.read_csv(s3_path)
            if 'calendar_dt' in df.columns and not df.empty:
                return df['calendar_dt'].iloc[0]
            else:
                print(f"No valid data found in Athena query output: {s3_path}")
                return None
        except Exception as e:
            print(f"Error reading Athena query output from S3: {e}")
            return None
    else:
        print(f"Error: Athena query failed with status {status}")
        return None

# Define BondEdge schema using StructType
def get_bondedge_schema():
    return StructType([
        StructField("CUSIP9", StringType(), True),
        StructField("Eff Dur", DoubleType(), True),
        StructField("Conv", DoubleType(), True),
        StructField("OAS", DoubleType(), True),
        StructField("Spread Dur", DoubleType(), True),
        StructField("Coupon", DoubleType(), True),
        StructField("Accrual", DoubleType(), True),
        StructField("Par Value", DoubleType(), True),
        StructField("1M KRD", DoubleType(), True),
        StructField("3M KRD", DoubleType(), True),
        StructField("6M KRD", DoubleType(), True),
        StructField("1Y KRD", DoubleType(), True),
        StructField("2Y KRD", DoubleType(), True),
        StructField("3Y KRD", DoubleType(), True),
        StructField("4Y KRD", DoubleType(), True),
        StructField("5Y KRD", DoubleType(), True),
        StructField("6Y KRD", DoubleType(), True),
        StructField("7Y KRD", DoubleType(), True),
        StructField("8Y KRD", DoubleType(), True),
        StructField("9Y KRD", DoubleType(), True),
        StructField("10Y KRD", DoubleType(), True),
        StructField("15Y KRD", DoubleType(), True),
        StructField("20Y KRD", DoubleType(), True),
        StructField("25Y KRD", DoubleType(), True),
        StructField("30Y KRD", DoubleType(), True),
        StructField("50Y KRD", DoubleType(), True)
    ])

# Define Bloomberg schema using StructType
def get_bloomberg_schema():
    return StructType([
        StructField("amica_portfolio_id", StringType(), True),
        StructField("amica_portfolio_desc", StringType(), True),
        StructField("pricing_dt", StringType(), True),
        StructField("cusip_id", StringType(), True),
        StructField("security_nm", StringType(), True),
        StructField("security_typ", StringType(), True),
        StructField("holdings_qty", DoubleType(), True),
        StructField("price_close_amt", DoubleType(), True),
        StructField("currency_typ", StringType(), True)
    ])

# Function to process BondEdge file
def process_bondedge(file_content, input_key, output_bucket):
    print(f"Processing BondEdge file: {input_key}")
    
    filename = input_key.split('/')[-1].split('.')[0]
    filename_dt = filename.split('r')[1]
    filename_dt = f"{filename_dt[:4]}-{filename_dt[4:6]}-{filename_dt[6:8]}"
    
    lines = file_content.split('\n')
    third_line = lines[2]
    pricing_dt = third_line.split(',')[1].strip()
    
    expected_date = athena_query_call(f"with selecteddayinfo as ( SELECT * FROM inv_a039986_test.inv_ref_trading_days_net_v where calendar_dt = date('{filename_dt}') and asset_class_desc = 'fixed income' ), target_trading_date as(SELECT trading_day_num - 1 AS target_trading_day_num FROM selecteddayinfo) select calendar_dt from inv_a039986_test.inv_ref_trading_days_net_v where trading_day_num = (select target_trading_day_num from target_trading_date) and asset_class_desc = 'fixed income';")
    
    if expected_date is None:
        print(f"Error: No result from Athena query for date {filename_dt}")
        return

    if pricing_dt.replace('-', '') != expected_date.replace('-', ''):
        print(f"Pricing date {pricing_dt} does not match expected date {expected_date}")
        return

    bondedge_schema = get_bondedge_schema()
    data_rdd = sc.parallelize(file_content.split("\n")[7:])
    df = spark.read.csv(data_rdd, header=False, schema=bondedge_schema)

    df = df.withColumn("pricing_dt", F.lit(pricing_dt).cast("date"))
    df = df.withColumn("inv_source_cd", F.lit("BDE").cast(StringType()))
    df = df.withColumn("load_ts", F.lit(datetime.now(timezone('America/New_York')).strftime("%Y-%m-%d %H:%M:%S")))
    df = df.withColumn("inv_source_file_nm", F.lit(input_key.split('/')[-1]))
    df = df.withColumn("inv_source_file_path_txt", F.lit(input_key))

    output_path = f"s3://{output_bucket}/amica-a039986-prahalad-investments/output_test_bucket/test-glue-investments/investments/inv_bondedge_test/{filename}.parquet"
    df.write.parquet(output_path, mode="overwrite")
    print(f"File saved at: {output_path}")

    crawler_trigger('inv_bondedge_test')

# Function to process Bloomberg file
def process_bloomberg(file_content, input_key, output_bucket):
    print(f"Processing Bloomberg file: {input_key}")
    
    filename = input_key.split('/')[-1].replace(".txt", '')
    filename_dt = f"20{filename.split('.')[-2]}"
    
    bloomberg_schema = get_bloomberg_schema()
    data_rdd = sc.parallelize(file_content.split("\n")[1:])
    df = spark.read.csv(data_rdd, header=False, schema=bloomberg_schema)

    df = df.withColumn("pricing_dt", F.lit(filename_dt).cast("date"))
    df = df.withColumn("inv_source_cd", F.lit("BBG").cast(StringType()))
    df = df.withColumn("load_ts", F.lit(datetime.now(timezone('America/New_York')).strftime("%Y-%m-%d %H:%M:%S")))
    df = df.withColumn("inv_source_file_nm", F.lit(input_key.split('/')[-1]))
    df = df.withColumn("inv_source_file_path_txt", F.lit(input_key))

    output_path = f"s3://{output_bucket}/amica-a039986-prahalad-investments/output_test_bucket/test-glue-investments/investments/inv_bloomberg_test/{filename}.parquet"
    df.write.parquet(output_path, mode="overwrite")
    print(f"File saved at: {output_path}")

    crawler_trigger('inv_bloomberg_crawler_test')

# Main process function
def process(input_bucket, input_key):
    output_bucket = 'amica-a035527-useast2-392523246457-dev'
    paginator = s3.get_paginator('list_objects_v2')
    response_iterator = paginator.paginate(Bucket=input_bucket, Prefix=input_key)

    for page in response_iterator:
        if 'Contents' in page:
            for obj in page['Contents']:
                input_key = obj['Key']
                if '.csv' in input_key or '.txt' in input_key:
                    print(f"Processing file: {input_key}")
                    content_object = s3.get_object(Bucket=input_bucket, Key=input_key)
                    file_content = content_object['Body'].read().decode('utf-8')

                    if 'bondedge' in input_key:
                        process_bondedge(file_content, input_key, output_bucket)
                    elif 'bloomberg' in input_key:
                        process_bloomberg(file_content, input_key, output_bucket)

# Main function
def main():
    input_bucket = 'amica-a035527-useast2-392523246457-dev'
    input_key = 'amica-a039986-prahalad-investments/test-investment-glue/bondedge/fixed-income/01/'
    input_key = 'amica-a039986-prahalad-investments/test-investment-glue/bloomberg/fixed-income/01/'
    process(input_bucket, input_key)

if __name__ == "__main__":
    main()
