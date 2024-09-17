import sys
import boto3
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, to_date, current_timestamp
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from datetime import datetime
import awswrangler as wr
import time

# Initialize GlueContext and SparkSession
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session
s3 = boto3.client('s3')
glue = boto3.client('glue')
athena = boto3.client('athena')
sns = boto3.client('sns')

# Input parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME', 's3_output_path', 'current_date', 's3_input_path'])
input_path = args['s3_input_path']
output_path = args['s3_output_path']
current_date = args['current_date']

# SNS Notification Function
def sns_notification(message):
    sns.publish(TopicArn='arn:aws:sns:us-east-2:392523246457:Failure_notification', Message=message)

# Glue Crawler Trigger Function
def crawler_trigger(crawler_name):
    try:
        glue.start_crawler(Name=crawler_name)
    except Exception as e:
        print(f"Error starting crawler: {e}")
        raise e

# Athena Query Execution Function
def athena_query_run(query, s3_loc, flag):
    response = athena.start_query_execution(
        QueryString=query,
        ResultConfiguration={'OutputLocation': s3_loc}
    )
    query_exec_id = response['QueryExecutionId']

    # Wait for the query to complete
    while True:
        status = athena.get_query_execution(QueryExecutionId=query_exec_id)['QueryExecution']['Status']['State']
        if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        time.sleep(5)

    if status == 'SUCCEEDED':
        bucket = s3_loc.split('/')[2]
        key = "/".join(s3_loc.split('/')[3:]) + query_exec_id + ".csv"
        obj = s3.get_object(Bucket=bucket, Key=key)
        df = pd.read_csv(obj['Body'], dtype=str if flag == 'output' else None)
        return df
    else:
        raise Exception("Athena query failed")

# Delete Athena output files from S3
def athena_output_del_func(s3_loc):
    objects_to_del = wr.s3.list_objects(s3_loc)
    if objects_to_del:
        wr.s3.delete_objects(objects_to_del)

# Process BondEdge and Bloomberg Files
def process_files(input_path, output_path):
    if 'bondedge' in input_path.lower():
        # Process BondEdge (.csv)
        df = spark.read.option("header", "true").csv(input_path)

        expected_columns = ['CUSIP9', 'Eff Dur', 'Conv', 'OAS', 'Spread Dur', 'Coupon', 'Accrual', 'Par Value',
                            '1M KRD', '3M KRD', '6M KRD', '1Y KRD', '2Y KRD', '3Y KRD', '4Y KRD', '5Y KRD',
                            '6Y KRD', '7Y KRD', '8Y KRD', '9Y KRD', '10Y KRD', '15Y KRD', '20Y KRD',
                            '25Y KRD', '30Y KRD', '50Y KRD']
        
        if len(df.columns) != 26 or df.columns != expected_columns:
            raise ValueError(f"Unexpected number of columns or column names in BondEdge file {input_path}")

        df_clean = df.withColumnRenamed('CUSIP9', 'cusip_id') \
                     .withColumnRenamed('Eff Dur', 'effective_duration') \
                     .withColumnRenamed('Conv', 'convexity') \
                     .withColumnRenamed('OAS', 'oas_bps') \
                     .withColumnRenamed('Spread Dur', 'spread_duration') \
                     .withColumnRenamed('Coupon', 'coupon_pct') \
                     .withColumn("pricing_dt", to_date(col("pricing_dt"), "yyyy-MM-dd")) \
                     .withColumn("load_ts", current_timestamp()) \
                     .withColumn("inv_source_cd", lit("BDE"))

        df_clean.write.mode("overwrite").parquet(f"{output_path}/processed_bondedge.parquet")
        crawler_trigger('inv_bondedge_crawler')

    elif 'bloomberg' in input_path.lower():
        # Process Bloomberg (.txt)
        df = spark.read.option("header", "true").option("delimiter", ",").csv(input_path)

        df_clean = df.withColumnRenamed('CUSIP9', 'cusip_id') \
                     .withColumnRenamed('Eff Dur', 'effective_duration') \
                     .withColumn("pricing_dt", to_date(col("pricing_dt"), "yyyy-MM-dd")) \
                     .withColumn("load_ts", current_timestamp()) \
                     .withColumn("inv_source_cd", lit("BBG"))

        df_clean.write.mode("overwrite").parquet(f"{output_path}/processed_bloomberg.parquet")
        crawler_trigger('inv_bloomberg_crawler')

# Main Processing Function
def process(current_date):
    # Query to get expected date (Athena query 1)
    query1 = f"""
    WITH selecteddayinfo AS (
        SELECT * FROM your_trading_days_table 
        WHERE calendar_dt = '{current_date}' 
        AND asset_class_desc = 'fixed income'
    ), target_trading_date AS (
        SELECT trading_day_num - 1 AS target_trading_day_num 
        FROM selecteddayinfo
    )
    SELECT calendar_dt FROM your_trading_days_table
    WHERE trading_day_num = (SELECT target_trading_day_num FROM target_trading_date) 
    AND asset_class_desc = 'fixed income';
    """
    s3_loc = f"s3://your-athena-output-bucket/{current_date}/"
    df1 = athena_query_run(query1, s3_loc, 'default')
    
    exp_date = df1['calendar_dt'][0]
    print(f'Expected date: {exp_date}')
    
    # Query for BondEdge and Bloomberg checks
    Bond_check_query = f"SELECT CASE WHEN COUNT(*)>0 THEN true ELSE false END AS check_flag FROM your_bondedge_table WHERE pricing_dt = date('{exp_date}');"
    bond_check_df = athena_query_run(Bond_check_query, s3_loc, 'default')
    flag_bond = bond_check_df['check_flag'][0]
    
    if flag_bond:
        Bloom_check_query = f"SELECT CASE WHEN COUNT(*)>0 THEN true ELSE false END AS check_flag FROM your_bloomberg_table WHERE pricing_dt = date('{exp_date}');"
        bloom_check_df = athena_query_run(Bloom_check_query, s3_loc, 'default')
        flag_bloom = bloom_check_df['check_flag'][0]
        
        if not flag_bloom:
            print(f'No Bloomberg data found for {exp_date}')
            sys.exit(0)
    else:
        print(f'No BondEdge data found for {exp_date}')
        sys.exit(0)

    # Final processing (Athena Query 3)
    Query3 = f"SELECT * FROM your_factset_table WHERE pricing_dt = '{exp_date}';"
    df_final = athena_query_run(Query3, s3_loc, 'output')
    
    # Transform and save
    df_spark = spark.createDataFrame(df_final)
    df_spark = df_spark.withColumn('load_ts', current_timestamp()) \
                       .withColumn('processing_date', lit(current_date))

    output_filepath = f"{output_path}/final_output_{current_date}.parquet"
    df_spark.write.mode("overwrite").parquet(output_filepath)

    crawler_trigger('your_glue_crawler_name')

# Main Glue Job Logic
def main():
    # Process BondEdge and Bloomberg files
    process_files(input_path, output_path)
    
    # Execute further data processing from Athena queries
    process(current_date)

# Entry point
if __name__ == "__main__":
    main()
