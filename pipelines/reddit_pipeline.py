from utils.constants import CLIENT_ID, SECRET, OUTPUT_PATH
from etls.reddit_etl import connect_reddit, extract_post, transform_data, load_data_to_csv

import pandas as pd

def reddit_pipeline(file_name: str, subreddit: str, time_filter='day', limit = None):
    #connect to reddit instance
    instance = connect_reddit(CLIENT_ID, SECRET, 'Airscholar Agent')

    #extraction
    posts = extract_post(instance, subreddit, time_filter, limit)
    df = pd.DataFrame(posts)
    
    #transformation
    df = transform_data(df)

    #loading to csv
    file_path = f'{OUTPUT_PATH}/{file_name}.csv'
    load_data_to_csv(df, file_path)

    return file_path