import sys
import boto3
import urllib.parse
import pandas as pd
from io import StringIO
import ast
import time
import awswrangler as wr
from datetime import datetime,date
import numpy as np
import csv
from pytz import timezone
from dateutil.rrule import DAILY, rrule, MO, TU, WE, TH, FR

s3 = boto3.client("s3")
s3_resource = boto3.resource("s3")
sns = boto3.client('sns')
glue = boto3.client("glue")
athena = boto3.client('athena')

#def sns_notification(message):
    #response = sns.publish(TopicArn='arn:aws:sns:us-east-2:392523246457:Failure_notification',
    #Message= message, )
    
def crawler_trigger(crawler_name):
    try:
       glue.start_crawler(Name=crawler_name)
    except Exception as e:
        print(e)
        print('Error starting crawler')
        raise e
        
def athena_query_run(Query, s3_loc, flag):
  
    response = athena.start_query_execution(QueryString = Query, QueryExecutionContext = {"Database": "inv_a039886_lambda_test"},
    ResultConfiguration = {"OutputLocation": s3_loc})
    QueryExecId = response["QueryExecutionId"]

    # Fetch data
    while True:
        response = athena.get_query_execution(QueryExecutionId = QueryExecId)
        status = response["QueryExecution"]["Status"]["State"]
        if status in ["SUCCEEDED","FAILED","CANCELLED"]:
            break
        time.sleep(1)
    
    df = pd.DataFrame()
    if status == "SUCCEEDED":
        bucket = s3_loc.split('/')[2]
        key = "/".join(s3_loc.split('/')[3:]) + QueryExecId + ".csv"
        obj = s3.get_object(Bucket = bucket,Key = key)
        if flag == 'output':
            df = pd.read_csv(obj['Body'], dtype = str)
        else:
            df = pd.read_csv(obj['Body'])
        
    return df

def athena_output_del_func(s3_loc):
    objects_to_del = wr.s3.list_objects(s3_loc)
    if objects_to_del:
        wr.s3.delete_objects(objects_to_del)
    
def process(current_date):
    # Get data from Athena
    # current_date for testing
    current_date = '2024-01-03'

    Query1 = f"with selecteddayinfo as ( SELECT * FROM inv_a039886_lambda_test.inv_ref_trading_days_net_v where calendar_dt = date('{current_date}') and asset_class_desc = 'fixed income' ), target_trading_date as(SELECT trading_day_num - 1 AS target_trading_day_num FROM selecteddayinfo) select calendar_dt from inv_a039886_lambda_test.inv_ref_trading_days_net_v where trading_day_num = (select target_trading_day_num from target_trading_date) and asset_class_desc = 'fixed income';"
    s3_loc = "s3://amica-a035527-useast2-392523246457-dev/amica-a039986-prahalad-investments/Athena_output/inv_export_output/pricing_dt_checker/"
    df1 = athena_query_run(Query1, s3_loc, 'First query')
    exp_date = df1['calendar_dt'][0]
    print('Expected date: ',exp_date)
    time.sleep(2)
    
    # # check both bloomberg and bondedge tables to see if the pricing dates exist in both
    Bond_check_query = f"select case when count(*)>0 then true else false end as check_flag from inv_a039886_lambda_test.inv_bondedge_daily_max_v where pricing_dt = date('{exp_date}');"
    s3_loc_ = "s3://amica-a035527-useast2-392523246457-dev/amica-a039986-prahalad-investments/check_query/"
    bond_check_df = athena_query_run(Bond_check_query, s3_loc_,'check')
    flag_bond = bond_check_df['check_flag'][0]

    if flag_bond == True:
        print(f'Bondedge data present for {exp_date}')
        athena_output_del_func(s3_loc_)    
        Bloom_check_query = f"select case when count(*)>0 then true else false end as check_flag from inv_a039886_lambda_test.inv_bloomberg_daily_max_v where pricing_dt = date('{exp_date}');"
        s3_loc_ = "s3://amica-a035527-useast2-392523246457-dev/amica-a039986-prahalad-investments/check_query/"
        bloom_check_df = athena_query_run(Bloom_check_query, s3_loc_, 'check')
        
        flag_bloom = bloom_check_df['check_flag'][0]
        if flag_bloom == True:
            print(f'Bloomberg data present for {exp_date}')
            athena_output_del_func(s3_loc_)
        else:
            athena_output_del_func(s3_loc_)
            print(f'No data for Bloomberg for pricing dt: {exp_date}. File not exported')
            sys.exit(0)
    else:
        athena_output_del_func(s3_loc_)
        print(f'No data for Bondedge for pricing dt: {exp_date}. File not exported')
        sys.exit(0)
        
    Query2 = f"select * from inv_a039886_lambda_test.inv_ref_trading_days_net_v where calendar_dt between date('{exp_date}') and date('{current_date}') and asset_class_desc = 'fixed income';"
    df2 = athena_query_run(Query2, s3_loc, 'intermediate')
    athena_output_del_func(s3_loc)
    
    try:
        holiday_dt = df2[df2['trading_date_typ'] == 'holiday']['calendar_dt'].iloc[0]
        print('Holiday present:',holiday_dt)
    except:
        print('No holiday noticed')
        holiday_dt = 0
    
    exp_date_str = str(exp_date).replace('-','')
    Query3 = f'select * from inv_a039886_lambda_test.inv_fia_factset_output_v where "pricing date"=\'{exp_date_str}\';'
    df3 = athena_query_run(Query3, s3_loc, 'output')
    athena_output_del_func(s3_loc)
    if len(df3)>0:
        if 0.0 in list(df3['Prices']) or '' in list(df3['Prices']):
            sns_notification("0.0 value for prices found. File not exported")
            sys.exit(0)
        
        duplicate_chk_df = pd.DataFrame()
        duplicate_chk_df['concatenated'] = df3['CUSIP9'] + str(df3['Pricing Date']) 
        duplicate_values = duplicate_chk_df['concatenated'].duplicated()

        if True in list(duplicate_values):
            sns_notification("Duplicate combination of CUSIP9 and pricing date found. File not exported")
            print("Duplicate combination of CUSIP9 and pricing date found. File not exported")
            sys.exit(0)
 
    # list of columns to be dropped when we export the file into S3
    col_drop_list = ['bondedge_source_file_nm','bloomberg_source_file_nm','bondedge_source_file_path_txt','bloomberg_source_file_path_txt','trading_day_num']
    df3 = df3.drop(columns = col_drop_list)
    
    df_final = pd.DataFrame()
    if holiday_dt == 0:
        df_final = df3
        print('Exporting the daily file')
    else:
        holiday_dt_str = str(holiday_dt).replace('-','')
        df4 = df3.copy()
        df4['Pricing Date'] = holiday_dt_str

        # Check Athena for the changes in prices for each cusip id and replace the value in df4(output holiday data)
        price_swap_query = f"select distinct cusip_id as CUSIP9, cast(price_close_amt as decimal(15,8))/100 as Prices from inv_bloomberg_daily_max_v where pricing_dt = date('{holiday_dt}') EXCEPT select distinct cusip_id as CUSIP9, cast(price_close_amt as decimal(15,8))/100 as Prices from inv_bloomberg_daily_max_v where pricing_dt = date('{exp_date}');"
        df_prices = athena_query_run(price_swap_query, s3_loc, 'output')
        athena_output_del_func(s3_loc)
        
        prices_map = df_prices.set_index('CUSIP9')['Prices'].to_dict()
        df4['Prices'] = df4['CUSIP9'].map(prices_map).combine_first(df4['Prices'])
        
        df_final = df3._append(df4, ignore_index=True)
        print('Exporting the file with holiday included')
        
    filename_ts_add = datetime.now(timezone('America/New_York')).strftime("%Y-%m-%d %H:%M:%S")
    
    output_filepath = f's3://amica-a035527-useast2-392523246457-dev/amica-a039986-prahalad-investments/output_test_bucket/factset-output/factset-landing/Outgoing/DailyFSAttr/DailyFSAttr_{current_date.replace('-','')}.csv'
    output_filepath2 = f's3://amica-a035527-useast2-392523246457-dev/amica-a039986-prahalad-investments/output_test_bucket/factset-output/inv_fia_factset_output_test/{current_date}/DailyFSAttr_{current_date.replace('-','')}_{filename_ts_add.replace(' ','_')}.csv'
    
    output_logging_filepath = f's3://amica-a035527-useast2-392523246457-dev/amica-a039986-prahalad-investments/output_test_bucket/factset-output/logging/DailyFSAttr_{current_date.replace('-','')}_{filename_ts_add}_logging.csv'
    
    df_logging = pd.DataFrame({'output_process_ts':filename_ts_add},[0])
    df_logging['pricing_dt_count'] = len(df_final['Pricing Date'].unique())

    unique_date_list = []
    for i in range(len(df_final['Pricing Date'].unique())):
        unique_date_list.append(int(df_final['Pricing Date'].unique()[i]))
    
    df_logging['min_pricing_dt'] = min(unique_date_list)
    df_logging['max_pricing_dt'] = max(unique_date_list)
    df_logging['row_count'] = len(df_final)
    df_logging['cusip_date_combo_count'] = len(df_final.drop_duplicates(['CUSIP9','Pricing Date']).index)
    df_logging['output_file_name'] = output_filepath.split('/')[-1]
    df_logging['output_file_path'] = output_filepath

    if len(df_final)> 0:    
        wr.s3.to_csv(df_final,output_filepath, index = False)
        wr.s3.to_csv(df_final,output_filepath2, index = False)
        
        wr.s3.to_csv(df_logging,output_logging_filepath, index = False)
    else:
        print('No data to be saved')
    
    athena_output_del_func(s3_loc)    
    
def daterange(start_date, end_date):
  return list(rrule(DAILY, dtstart=start_date, until=end_date, byweekday=(MO,TU,WE,TH,FR)))


def lambda_handler(event,context):
    # # Daily run
    #response = process(event['current_date'])
    # response = process('2024-05-22')
    #crawler_trigger('inv_output_factset_csv_crawler')
    # crawler_trigger('inv_logging_output')
    
    # # For Backfilling
    start = datetime(2024, 1, 1)
    end = datetime(2024, 1, 10)
    
    datelist = daterange(start,end)
    datelist = [day.strftime("%Y-%m-%d") for day in datelist]
    for i in datelist:
         current_date = i
         print(i)
         process(current_date)
    crawler_trigger('inv_factset_csv_test')
    
    
#     Dev Account: s3://amica-aws-datalake-general-dev-factset-landing/Outgoing/DailyFSAttr/
# Prod Account: s3://amica-aws-datalake-general-prod-factset-landing/Outgoing/DailyFSAttr/