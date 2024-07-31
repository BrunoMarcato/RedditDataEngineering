import numpy as np
import pandas as pd
from praw import Reddit

from utils.constants import POST_FIELDS

# ---

def extract_data(reddit_instance: Reddit, subreddit: str, time_filter: str, limit=None):
    subreddit = reddit_instance.subreddit(subreddit)
    posts = subreddit.top(time_filter=time_filter, limit=limit)

    post_lists = []

    for post in posts:
        post_dict = vars(post)
        post = {key: post_dict[key] for key in POST_FIELDS}
        post_lists.append(post)

    return post_lists

# ---

def transform_data(post_df: pd.DataFrame):
    post_df = pd.DataFrame(post_df)
    post_df['id'] = post_df['id'].astype(str)
    post_df['created_utc'] = pd.to_datetime(post_df['created_utc'], unit='s')
    post_df['over_18'] = post_df['over_18'].astype(bool)
    post_df['author'] = post_df['author'].astype(str)
    post_df['edited'] = np.where(post_df['edited'],True,False)
    post_df['num_comments'] = post_df['num_comments'].astype(int)
    post_df['score'] = post_df['score'].astype(int)
    post_df['title'] = post_df['title'].astype(str)

    return post_df

# ---

# Loads data to csv file
def load_data(data: pd.DataFrame, path: str):
    data.to_csv(path, index=False)
