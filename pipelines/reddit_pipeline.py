from etls.reddit_etl import extract_data, transform_data, load_data 
from utils.constants import CLIENT_ID, SECRET, OUTPUT_PATH
from utils.connect_reddit import connect_reddit
#import pandas as pd


def reddit_pipeline(filename, subreddit, time_filter='day', limit=None):
    
    # Connecting to Reddit API
    reddit_instance = connect_reddit(CLIENT_ID, SECRET, 'testscript')

    # ETL pipeline (Extraction, Transform and Load)

    # 1) Extraction - E

    posts = extract_data(reddit_instance, subreddit, time_filter, limit)

    # 2) Transform - T

    transformed_posts = transform_data(posts)

    # 3) Load - L

    load_data(transformed_posts, path=f'{OUTPUT_PATH}/{filename}.csv')