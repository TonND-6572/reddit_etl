import sys
import praw
import pandas as pd
import numpy as np

from utils.constants import POST_FIELDS

def connect_reddit(client_id, client_secret, user_agent):
    try:    
        reddit = praw.Reddit(
            client_id = client_id,
            client_secret = client_secret,
            user_agent = user_agent,
        )
        print('Connect to reddit success')
        return reddit
    except Exception as e:
        print('Connect fail')
        sys.exit(1)

def extract_post(reddit_instance: praw.Reddit, subreddit:str, time_filter:str, limit=None):
    subreddit = reddit_instance.subreddit(subreddit)
    posts = subreddit.top(time_filter=time_filter, limit=limit)
    post_list = []
    
    for post in posts:
        post_dict = vars(post)
        
        post = {key: post_dict[key] for key in POST_FIELDS}
        post_list.append(post)
    
    return post_list

def fill_invalid_bool(df, col_name):
    col_mode = df[col_name].mode()
    df[col_name] = np.where(df[col_name].isin([True, False]), 
                                      df[col_name],
                                      col_mode).astype(bool)
    
    return df[col_name]

def transform_data(post_df: pd.DataFrame):
    post_df['title'] = post_df['title'].astype(str)
    post_df['score'] = post_df['score'].astype(int)
    post_df['num_comments'] = post_df['num_comments'].astype(int)
    post_df['author'] = post_df['author'].astype(str)
    post_df['created_utc'] = pd.to_datetime(post_df['created_utc'], unit='s')
    post_df['over_18'] = np.where((post_df['over_18'] == True), True, False)
    
    post_df['edited'] = fill_invalid_bool(post_df, 'edited')
    post_df['spoiler'] = fill_invalid_bool(post_df, 'spoiler')
    post_df['stickied'] = fill_invalid_bool(post_df, 'stickied')
    
    return post_df

def load_data_to_csv(data: pd.DataFrame, path:str):
    data.to_csv(path, index=False)