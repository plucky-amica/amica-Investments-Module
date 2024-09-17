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

s3 = boto3.client("s3")
s3_resource = boto3.resource("s3")
sns = boto3.client('sns')
glue = boto3.client("glue")
athena = boto3.client('athena')
lambda_client = boto3.client('lambda')

def sns_notification(error_type, message):
    # Function to call the sns notification in the case of a failure 
    if error_type == 'condition_error':
        response = sns.publish(TopicArn='arn:aws:sns:us-east-2:392523246457:Failure_notification',
        Message= message, )
    if error_type == 'critical':
        print(f'Error type: {error_type}, {message}')
        response = sns.publish(TopicArn='arn:aws:sns:us-east-2:392523246457:Failure_notification',
        Message= message, )
        
def crawler_trigger(crawler_name):
    # Function to call the crawlers
    try:
       glue.start_crawler(Name=crawler_name)
    except Exception as e:
        print(e)
        print('Error starting crawler')
        raise e
        
def athena_query_call(Query):
    # Function to run queries on Athena and fetch the data 
    s3_save_loc = "s3://amica-aws-eds-dev-engineers-test/inv_export_output/pricing_dt_checker/"
    response = athena.start_query_execution(QueryString = Query, QueryExecutionContext = {"Database": "amica_eds_dev_engineers_test_db"},
    ResultConfiguration = {"OutputLocation": s3_save_loc})
    QueryExecId = response["QueryExecutionId"]

    # Fetch data
    while True:
        response = athena.get_query_execution(QueryExecutionId = QueryExecId)
        status = response["QueryExecution"]["Status"]["State"]
        if status in ["SUCCEEDED","FAILED","CANCELLED"]:
            break
        time.sleep(1)
    
    if status == "SUCCEEDED":
        bucket = s3_save_loc.split('/')[2]
        key = "/".join(s3_save_loc.split('/')[3:]) + QueryExecId + ".csv"
        obj = s3.get_object(Bucket = bucket,Key = key)
        df = pd.read_csv(obj['Body'])
        exp_date = df['calendar_dt'][0]
        objects_to_del = wr.s3.list_objects(s3_save_loc)
        if objects_to_del:
            wr.s3.delete_objects(objects_to_del)
        return exp_date
        
def rename_reorder_func(df, string_cols, int_cols, double_cols, date_cols, rename_dict, reorder_list):
    # Function for column data type assignment, renaming and reordering 
    for i in string_cols:
        df[i] = df[i].astype('str')
        df[i] = df[i].str.strip()
        if i not in ['inv_source_file_nm','inv_source_file_path_txt']:
            df[i] = df[i].str.upper()
        
    if len(int_cols)>0:    
        for i in int_cols:
            df[i] = df[i].fillna(0)
            df[i] = df[i].astype('int64')
            
    if len(double_cols)>0:    
        for i in double_cols:
            df[i] = pd.to_numeric(df[i]).astype('float64')
            # df[i] = df[i].apply(pd.to_numeric, args=('coerce',))
    if len(date_cols)>0:
        for i in date_cols:
            df[i] = pd.to_datetime(df[i]).dt.date
            
    if len(rename_dict)>0: 
        df = df.rename(columns =rename_dict)
        
    df = df.reindex(columns = reorder_list)
     
    return df
    

def process(input_bucket, input_key):
    output_bucket = 'amica-aws-eds-dev-engineers-test'
    paginator = s3.get_paginator('list_objects_v2')
    response_iterator = paginator.paginate(Bucket= input_bucket, Prefix = input_key)
    
    filename_dt_parameter = ''
    
    for page in response_iterator:
        if 'Contents' in page:
            for obj in page['Contents']:
                input_key = obj['Key']
                if '.csv' in input_key or '.txt' in input_key:
                    print(input_key)
                    filename = input_key.split('/')[-1].split('.')[0]
                    content_object = s3.get_object(Bucket=input_bucket, Key = input_key)
                    
                    file_content = content_object['Body'].read().decode('utf-8')
                    
                    # Bondedge process 
                    if 'bondedge' in input_key:
                        print('BondEdge process')
                        filename_dt = filename.split('r')[1]
                        filename_dt = filename_dt[:4] + '-' + filename_dt[4:6] + '-' + filename_dt[6:8]
                        filename_dt_parameter = filename_dt
                        
                        lines = file_content.split('\n')
                        third_line = lines[2]
                        data = StringIO(file_content)
                        receiving_order_list = ['CUSIP9', 'Eff Dur', 'Conv', 'OAS', 'Spread Dur', 'Coupon', 'Accrual', 'Par Value', '1M KRD', '3M KRD', '6M KRD', '1Y KRD', '2Y KRD', '3Y KRD', '4Y KRD', '5Y KRD', '6Y KRD', '7Y KRD', '8Y KRD', '9Y KRD', '10Y KRD', '15Y KRD', '20Y KRD', '25Y KRD', '30Y KRD', '50Y KRD']

                        try:
                            df = pd.read_csv(data, skiprows = 7, dtype={'CUSIP9':'string', 'Eff Dur':'float64', 'Conv':'float64', 'Spread Dur':'float64', 'Coupon':'float64', 'Accrual':'string', 'Par Value':'int64', '1M KRD':'float64', '3M KRD':'float64', '6M KRD':'float64', '1Y KRD':'float64', '2Y KRD':'float64', '3Y KRD':'float64', '4Y KRD':'float64', '5Y KRD':'float64', '6Y KRD':'float64', '7Y KRD':'float64', '8Y KRD':'float64', '9Y KRD':'float64', '10Y KRD':'float64', '15Y KRD':'float64', '20Y KRD':'float64', '25Y KRD':'float64', '30Y KRD':'float64', '50Y KRD':'float64'},skipfooter=1, sep = ',',quotechar = '"',thousands = ',',skipinitialspace=True, engine='python')
                        except Exception as e:
                            sns_notification('critical',f'Unreadable BondEdge CSV: {input_key}')
                            print(e)
                            sys.exit(0)
                        
                        pricing_dt = (third_line.split(',')[1]).replace(" ", "")
                        print("pricing_dt=",pricing_dt)
                        pricing_dt_check = pricing_dt[6:10] + pricing_dt[0:2] + pricing_dt[3:5]
                        print(pricing_dt_check) 
                        
                        # Getting expected date from Athena reference table based on the date in the filename  
                        expected_date = athena_query_call(f"with selecteddayinfo as ( SELECT * FROM amica_eds_dev_engineers_test_db.inv_ref_trading_days_net_v where calendar_dt = date('{filename_dt}') and asset_class_desc = 'fixed income' ), target_trading_date as(SELECT trading_day_num - 1 AS target_trading_day_num FROM selecteddayinfo) select calendar_dt from amica_eds_dev_engineers_test_db.inv_ref_trading_days_net_v where trading_day_num = (select target_trading_day_num from target_trading_date) and asset_class_desc = 'fixed income';")
                        expected_date = expected_date.replace('-','')
                        
                        if pricing_dt_check != expected_date:
                            sns_notification('condition_error',f'The BondEdge pricing date {pricing_dt_check} in file {input_key} does not match with the expected date {expected_date}')
                            print(f'The BondEdge pricing date {pricing_dt_check} in file {input_key} does not match with the expected date {expected_date}')
                            sys.exit(0)
                        print('Pricing date matches the expected date')
                        
                        if df.empty:
                            sns_notification('condition_error',f'The BondEdge file {input_key} is empty')
                            print(f'The BondEdge file {input_key} is empty')
                            sys.exit(0)
                        print('passed df empty check')
                        
                        if len(df.columns) != 26:
                            sns_notification('condition_error',f'The BondEdge file {input_key} has unexpected number of columns')
                            print(f'The BondEdge file {input_key} has unexpected number of columns')
                            sys.exit(0)
                        print('count of columns in df check passed')
                        
                        if list(df.columns) != receiving_order_list:
                            sns_notification('condition_error',f'The BondEdge file {input_key} has unexpected order of columns or column name different than expected')
                            print(f'The BondEdge file {input_key} has unexpected order of columns or column name different than expected')
                            sys.exit(0)
                        print('order of columns in df check passed')
                        
                        df['pricing_dt'] = pd.to_datetime(pricing_dt)
                        
                        # Assigning source code to differentiate between manual and daily runs
                        if 'manual' in input_key:
                            df['inv_source_cd'] = 'BDE-X'
                        else:
                            df['inv_source_cd'] = 'BDE'
                        
                        # Assigning values for user assigned columns
                        df['load_ts'] = pd.to_datetime(datetime.now(timezone('America/New_York')).strftime("%Y-%m-%d %H:%M:%S"))
                        df['inv_source_file_nm'] = input_key.split('/')[-1]
                        df['inv_source_file_path_txt'] = input_key
                        
                        # Column data type assignment and reordering
                        string_cols = ['CUSIP9','inv_source_cd', 'Accrual','inv_source_file_nm', 'inv_source_file_path_txt']
                        int_cols = ['Par Value','OAS']
                        double_cols = ['Eff Dur', 'Conv', 'Spread Dur', 'Coupon', '1M KRD', '3M KRD', '6M KRD', '1Y KRD', '2Y KRD', '3Y KRD', '4Y KRD', '5Y KRD', '6Y KRD', '7Y KRD', '8Y KRD', '9Y KRD', '10Y KRD', '15Y KRD', '20Y KRD', '25Y KRD', '30Y KRD', '50Y KRD']
                        date_cols = ['pricing_dt']
                        rename_dict = {'CUSIP9':'cusip_id', 'Eff Dur':'effective_duration', 'Conv':'convexity', 'OAS':'oas_bps', 'Spread Dur':'spread_duration', 'Coupon':'coupon_pct', 'Accrual':'accrual_amt', 'Par Value':'par_value', '1M KRD':'KRD_01M', '3M KRD':'KRD_03M', '6M KRD':'KRD_06M', '1Y KRD':'KRD_01Y', '2Y KRD':'KRD_02Y', '3Y KRD':'KRD_03Y', '4Y KRD':'KRD_04Y', '5Y KRD':'KRD_05Y', '6Y KRD':'KRD_06Y', '7Y KRD':'KRD_07Y', '8Y KRD':'KRD_08Y', '9Y KRD':'KRD_09Y', '10Y KRD':'KRD_10Y', '15Y KRD':'KRD_15Y', '20Y KRD':'KRD_20Y', '25Y KRD':'KRD_25Y', '30Y KRD':'KRD_30Y', '50Y KRD':'KRD_50Y'}
                        reorder_list = ['inv_source_cd', 'inv_source_file_nm', 'pricing_dt', 'cusip_id', 'effective_duration', 'convexity', 'oas_bps', 'spread_duration', 'coupon_pct', 'accrual_amt', 'par_value', 'KRD_01M', 'KRD_03M', 'KRD_06M', 'KRD_01Y', 'KRD_02Y', 'KRD_03Y', 'KRD_04Y', 'KRD_05Y', 'KRD_06Y', 'KRD_07Y', 'KRD_08Y', 'KRD_09Y', 'KRD_10Y', 'KRD_15Y', 'KRD_20Y', 'KRD_25Y', 'KRD_30Y', 'KRD_50Y','inv_source_file_path_txt','load_ts']
                        
                        df = rename_reorder_func(df, string_cols, int_cols, double_cols, date_cols, rename_dict, reorder_list)
                        
                        replace_chars = ['NULL','N/A']
                        for i in replace_chars:
                            df = df.replace(i,'')
                            
                        # Saving the processed Bondedge data
                        wr.s3.to_parquet(df, path=f"s3://{output_bucket}/investments/inv_bondedge/{filename}.snappy.parquet", index=False, compression="snappy")
                        print(f"s3://{output_bucket}/investments/inv_bondedge/{filename}.snappy.parquet has been saved")
                        
                        # crawler_trigger('inv_bondedge_crawler')
                        
                    # Bloomberg process
                    elif 'bloomberg' in input_key:
                        print('Bloomberg process')
                        filename = input_key.split('/')[-1].replace(".txt",'')
                        filename_dt = '20' + filename.split('.')[-2]
                        filename_dt = filename_dt[0:4] + '-' + filename_dt[4:6] + '-' + filename_dt[6:]
                        filename_dt_parameter = filename_dt
                        
                        data = StringIO(file_content)
                        col_names = ['amica_portfolio_id','amica_portfolio_desc','pricing_dt','cusip_id','security_nm','security_typ','holdings_qty','price_close_amt','currency_typ']
                        try:
                            df = pd.read_csv(data, sep = ',', names= col_names, dtype = {'amica_portfolio_id':'str','amica_portfolio_desc':'str','pricing_dt':'str','cusip_id':'str','security_nm':'str','security_typ':'str','holdings_qty':'float64','price_close_amt':'float64','currency_typ':'str'}, header = None, skiprows = 1, skipfooter=1, skipinitialspace=True)
                        except Exception as e:
                            sns_notification('critical',f'Unreadable Bloomberg text file: {input_key}')
                            print(e)
                            sys.exit(0)

                        if df.empty:
                            sns_notification('condition_error',f'The Bloomberg file: {input_key} is empty')
                            print('condition_error', f'The Bloomberg file: {input_key} is empty')
                            sys.exit(0)
                        print('passed df empty check')
                        
                        # Assigning source code to differentiate between manual and daily runs
                        if 'manual' in input_key:
                            df['inv_source_cd'] = 'BBG-X'
                        else:
                            df['inv_source_cd'] = 'BBG'
                        
                        # Assigning values for user assigned columns
                        df['load_ts'] = pd.to_datetime(datetime.now(timezone('America/New_York')).strftime("%Y-%m-%d %H:%M:%S"))
                        df['inv_source_file_nm'] = input_key.split('/')[-1]
                        df['inv_source_file_path_txt'] = input_key
                        
                        # Column data type assignment and reordering
                        string_cols = ['inv_source_file_path_txt','inv_source_file_nm','amica_portfolio_id','amica_portfolio_desc','cusip_id','security_nm','security_typ','currency_typ', 'inv_source_cd']
                        double_cols = ['holdings_qty','price_close_amt']
                        int_cols = []
                        date_cols = ['pricing_dt']
                        rename_dict = {}
                        reorder_list = ['inv_source_cd', 'inv_source_file_nm', 'pricing_dt','cusip_id','security_nm','security_typ','holdings_qty','price_close_amt','currency_typ','amica_portfolio_id','amica_portfolio_desc','inv_source_file_path_txt','load_ts']

                        for i in string_cols:
                            if df[i].isnull().any():
                                sns_notification('condition_error',f'The Bloomberg file {input_key} has unexpected number of columns(string_cols have None)')
                                print('condition_error', f'The Bloomberg file {input_key} has unexpected number of columns(string_cols have None)')
                                sys.exit(0)
                                
                        df = rename_reorder_func(df, string_cols, int_cols, double_cols, date_cols, rename_dict, reorder_list)
                        pricing_dt = str(df['pricing_dt'][0])
                        print('df reordered and datatype assigned')
                        
                        replace_chars = ['NULL','N/A']
                        for i in replace_chars:
                            df = df.replace(i,'')
                            
                        # Saving the processed Bondedge data after checking if the date in the filename and pricing date matches
                        if pricing_dt == filename_dt:
                            wr.s3.to_parquet(df, path=f"s3://{output_bucket}/investments/inv_bloomberg/{filename}.snappy.parquet", index=False, compression="snappy")
                            print(f"s3://{output_bucket}/investments/inv_bloomberg/{filename}.snappy.parquet has been saved")
                            
                            # crawler_trigger('inv_bloomberg_crawler')
                        else:
                            sns_notification('condition_error', f'The pricing date {pricing_dt} in file {input_key}, does not match the date in the file name {filename_dt}')
                            print(f'The pricing date {pricing_dt} in file {input_key}, does not match the date in the file name {filename_dt}')
                            sys.exit(0)
                            
    return filename_dt_parameter
    
def lambda_handler(event,context):
    input_bucket = event['Records'][0]['s3']['bucket']['name']
    input_key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key']) 
    
    # # Test input script
    # input_bucket = 'amica-aws-eds-dev-raw-refined'
    # input_key = 'bondedge-attribution/year=2024/month=09/day=09/DailyFSAttr20240910.csv'
    # process(input_bucket, input_key)
    
    filename_dt = process(input_bucket, input_key)
    
    if 'bondedge' in input_key:
        payload = {'current_date':filename_dt}
        payload_json = json.dumps(payload)
        
        response = lambda_client.invoke(
            FunctionName='arn:aws:lambda:us-east-2:392523246457:function:rravichandran_test_fi_export',
            InvocationType = 'Event',
            Payload = payload_json 
            )