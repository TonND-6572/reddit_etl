import sys
import boto3
import time

import pandas as pd

def connect_to_botos3(client:str) -> boto3.session.Session.client:
    try:
        return boto3.client(client)
    except Exception as e:
        print('Connect failed! Check your client name or aws configure')
        sys.exit(1)

# run glue etl script and pass parameter to get the last csv file
def glue_etl(client:boto3.session.Session.client, job_name:str):
    try:
        client.get_job(job_name)
        start_job_run_id = client.start_job_run(
            JobName=job_name,
            Timeout=10)
        if start_job_run_id["ResponseMetadata"]["HTTPStatusCode"]:
            print('start job successfully')
            return start_job_run_id['JobRunId']
    except Exception as e:
        print(e)
        sys.exit(1)

def etl_done(client:boto3.session.Session.client, job_name, run_id):
    try:
        res = client.get_job_run(JobName = job_name, RunId = run_id)
        if res['JobRun']['JobRunState'] == 'SUCCEEDED':
            return True
        else:
            print(res['JobRun']['JobRunState'])
            return False
    except Exception as e:
        print(e)

# run glue crawler
def glue_crawler(client:boto3.session.Session.client, crawler_name:str):
    try:
        client.start_crawler(Name=crawler_name)
        print('start glue crawler successfully')
    except Exception as e:
        print('check crawler name')

# get data from aws datacatalog
def get_from_data_catalog(client:boto3.session.Session.client):
    response = client.start_query_execution(
        QueryString = 'SELECT * FROM "AwsDataCatalog"."reddit_db"."tbl_reddit_transformed"',
        QueryExecutionContext={
            'Database': 'reddit_db',
            'Catalog': 'hive'
        },
        ResultConfiguration = {
            'OutputLocation': 's3://wakuwa-athena-bucket-result/Unsaved/'
        },
        WorkGroup = 'primary',
        ResultReuseConfiguration={
            'ResultReuseByAgeConfiguration': {
                'Enabled': True,
                'MaxAgeInMinutes': 10
            }
        }
    )
    time.sleep(6)
    result = client.get_query_results(QueryExecutionId=response['QueryExecutionId'])

    return result
     

def extract_data(data:dict):
    def extract(row):
        return [col_name["VarCharValue"] for col_name in row["Data"]]
    
    rows = data["ResultSet"]["Rows"]
    col_name = extract(rows[0])
    data_list = []
    for row in rows[1:]:
        try:
            data_list.append(extract(row))
        except:
            print(row)
    
    return pd.DataFrame(data=data_list, columns=col_name)