import sys
import boto3
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from datetime import datetime, date
import awswrangler as wr
import time
from pytz import timezone
from dateutil.rrule import DAILY, rrule, MO, TU, WE, TH, FR

# Initialize GlueContext and SparkSession
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session
s3 = boto3.client('s3')
sns = boto3.client('sns')
glue = boto3.client('glue')
athena = boto3.client('athena')

# Input parameters for Glue Job
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'current_date', 's3_output_path'])
current_date = args['current_date']
output_path = args['s3_output_path']

# SNS Notification
def sns_notification(message):
    sns.publish(TopicArn='arn:aws:sns:us-east-2:392523246457:Failure_notification', Message=message)

# Glue Crawler Trigger
def crawler_trigger(crawler_name):
    try:
        glue.start_crawler(Name=crawler_name)
    except Exception as e:
        print(f"Error starting crawler: {e}")
        raise e

# Athena Query Execution
def athena_query_run(query, s3_loc, flag):
    response = athena.start_query_execution(
        QueryString=query,
        ResultConfiguration={"OutputLocation": s3_loc}
    )
    query_exec_id = response['QueryExecutionId']

    # Wait for query completion
    while True:
        status = athena.get_query_execution(QueryExecutionId=query_exec_id)['QueryExecution']['Status']['State']
        if status in ["SUCCEEDED", "FAILED", "CANCELLED"]:
            break
        time.sleep(5)

    df = pd.DataFrame()
    if status == "SUCCEEDED":
        bucket = s3_loc.split('/')[2]
        key = "/".join(s3_loc.split('/')[3:]) + query_exec_id + ".csv"
        obj = s3.get_object(Bucket=bucket, Key=key)
        if flag == 'output':
            df = pd.read_csv(obj['Body'], dtype=str)
        else:
            df = pd.read_csv(obj['Body'])
    return df

# Delete Athena output from S3
def athena_output_del_func(s3_loc):
    objects_to_del = wr.s3.list_objects(s3_loc)
    if objects_to_del:
        wr.s3.delete_objects(objects_to_del)

# Main processing function
def process(current_date):
    # Athena Query 1
    query1 = f"""
    WITH selecteddayinfo AS (
        SELECT * FROM amica_eds_dev_engineers_test_db.inv_ref_trading_days_net_v
        WHERE calendar_dt = date('{current_date}') AND asset_class_desc = 'fixed income'
    ), target_trading_date AS (
        SELECT trading_day_num - 1 AS target_trading_day_num FROM selecteddayinfo
    )
    SELECT calendar_dt FROM amica_eds_dev_engineers_test_db.inv_ref_trading_days_net_v
    WHERE trading_day_num = (SELECT target_trading_day_num FROM target_trading_date)
    AND asset_class_desc = 'fixed income';
    """
    s3_loc = f"s3://amica-aws-eds-dev-engineers-test/inv_export_output/"
    df1 = athena_query_run(query1, s3_loc, 'First query')
    exp_date = df1['calendar_dt'][0]
    print(f"Expected date: {exp_date}")
    
    # Check BondEdge and Bloomberg data for the expected date
    bond_check_query = f"""
    SELECT CASE WHEN COUNT(*) > 0 THEN TRUE ELSE FALSE END AS check_flag
    FROM amica_eds_dev_engineers_test_db.inv_bondedge WHERE pricing_dt = date('{exp_date}');
    """
    bond_check_df = athena_query_run(bond_check_query, s3_loc, 'check')
    flag_bond = bond_check_df['check_flag'][0]
    
    if flag_bond:
        bloom_check_query = f"""
        SELECT CASE WHEN COUNT(*) > 0 THEN TRUE ELSE FALSE END AS check_flag
        FROM amica_eds_dev_engineers_test_db.inv_bloomberg WHERE pricing_dt = date('{exp_date}');
        """
        bloom_check_df = athena_query_run(bloom_check_query, s3_loc, 'check')
        flag_bloom = bloom_check_df['check_flag'][0]
        
        if not flag_bloom:
            print(f"No data for Bloomberg for pricing date: {exp_date}. File not exported.")
            sys.exit(0)
    else:
        print(f"No data for BondEdge for pricing date: {exp_date}. File not exported.")
        sys.exit(0)

    # Further queries and processing
    query2 = f"""
    SELECT * FROM amica_eds_dev_engineers_test_db.inv_ref_trading_days_net_v
    WHERE calendar_dt BETWEEN date('{exp_date}') AND date('{current_date}')
    AND asset_class_desc = 'fixed income';
    """
    df2 = athena_query_run(query2, s3_loc, 'intermediate')
    athena_output_del_func(s3_loc)

    # Handle holidays
    try:
        holiday_dt = df2[df2['trading_date_typ'] == 'holiday']['calendar_dt'].iloc[0]
        print(f"Holiday detected: {holiday_dt}")
    except:
        print("No holidays detected.")
        holiday_dt = 0

    # Query for factset output
    exp_date_str = exp_date.replace('-', '')
    query3 = f"""
    SELECT * FROM amica_eds_dev_engineers_test_db.inv_fia_factset_output_v
    WHERE "pricing date" = '{exp_date_str}';
    """
    df3 = athena_query_run(query3, s3_loc, 'output')
    athena_output_del_func(s3_loc)
    
    # Handle pricing errors and duplicates
    if len(df3) > 0:
        if (df3['Prices'] == '0.0').any() or (df3['Prices'] == '').any():
            sns_notification("0.0 value for prices found. File not exported.")
            sys.exit(0)
        
        df3['concatenated'] = df3['CUSIP9'] + df3['Pricing Date'].astype(str)
        if df3['concatenated'].duplicated().any():
            sns_notification("Duplicate combination of CUSIP9 and pricing date found. File not exported.")
            sys.exit(0)
    
    # Drop unwanted columns
    col_drop_list = ['bondedge_source_file_nm', 'bloomberg_source_file_nm', 'bondedge_source_file_path_txt', 'bloomberg_source_file_path_txt', 'trading_day_num']
    df3 = df3.drop(columns=col_drop_list)
    
    # Final DataFrame preparation
    df_final = pd.DataFrame()
    if holiday_dt == 0:
        df_final = df3
        print("Exporting daily file.")
    else:
        holiday_dt_str = str(holiday_dt).replace('-', '')
        df4 = df3.copy()
        df4['Pricing Date'] = holiday_dt_str

        price_swap_query = f"""
        SELECT DISTINCT cusip_id AS CUSIP9, CAST(price_close_amt AS DECIMAL(15, 8)) / 100 AS Prices
        FROM inv_bloomberg_daily_max_v WHERE pricing_dt = date('{holiday_dt}')
        EXCEPT
        SELECT DISTINCT cusip_id AS CUSIP9, CAST(price_close_amt AS DECIMAL(15, 8)) / 100 AS Prices
        FROM inv_bloomberg_daily_max_v WHERE pricing_dt = date('{exp_date}');
        """
        df_prices = athena_query_run(price_swap_query, s3_loc, 'output')
        athena_output_del_func(s3_loc)
        
        price_map = df_prices.set_index('CUSIP9')['Prices'].to_dict()
        df4['Prices'] = df4['CUSIP9'].map(price_map).combine_first(df4['Prices'])
        
        df_final = pd.concat([df3, df4], ignore_index=True)
        print("Exporting file with holiday included.")
    
    # Save final data to S3
    timestamp = datetime.now(timezone('America/New_York')).strftime("%Y-%m-%d %H:%M:%S")
    output_filepath = f"s3://your-bucket-name/Outgoing/DailyFSAttr/DailyFSAttr_{current_date}.csv"
    output_filepath_logging = f"s3://your-bucket-name/logging/DailyFSAttr_{current_date}_{timestamp}.csv"
    
    wr.s3.to_csv(df_final, path=output_filepath, index=False)
    
    # Save logging information
    log_data = {
        'output_process_ts': timestamp,
        'pricing_dt_count': len(df_final['Pricing Date'].unique()),
        'min_pricing_dt': min(df_final['Pricing Date'].unique()),
        'max_pricing_dt': max(df_final['Pricing Date'].unique()),
        'row_count': len(df_final),
        'cusip_date_combo_count': len(df_final.drop_duplicates(['CUSIP9', 'Pricing Date']).index),
        'output_file_name': output_filepath.split('/')[-1],
        'output_file_path': output_filepath
    }
    df_logging = pd.DataFrame([log_data])
    wr.s3.to_csv(df_logging, path=output_filepath_logging, index=False)

# Backfill Functionality (Optional)
def daterange(start_date, end_date):
    return list(rrule(DAILY, dtstart=start_date, until=end_date, byweekday=(MO, TU, WE, TH, FR)))

# Main Glue Job Function
def main():
    process(current_date)
    crawler_trigger('your_glue_crawler_name')

# Entry Point
if __name__ == "__main__":
    main()
