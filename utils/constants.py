'''
    File to get information from config file

    Essas informações são usadas no arquivo reddit_pipeline.py
    These informations are used in reddit_pipeline.py file
'''

import configparser
import os

parser = configparser.ConfigParser()
parser.read(os.path.join(os.path.dirname(__file__), '../config/config.conf'))

# Getting database information

DATABASE_HOST = parser.get('database', 'database_host')
DATABASE_NAME = parser.get('database', 'database_name')
DATABASE_PORT = parser.get('database', 'database_port')
DATABASE_USERNAME = parser.get('database', 'database_username')
DATABASE_PASSWORD = parser.get('database', 'database_password')

# Getting Reddit API information
SECRET = parser.get('api_keys', 'reddit_secret_key')
CLIENT_ID = parser.get('api_keys', 'reddit_client_id')
REDDIT_USERNAME = parser.get('api_keys', 'reddit_username')
REDDIT_PW = parser.get('api_keys', 'reddit_password')

# Getting data paths information
INPUT_PATH = parser.get('file_paths', 'input_path')
OUTPUT_PATH = parser.get('file_paths', 'output_path')

POST_FIELDS = (
    'id',
    'title',
    'score',
    'num_comments',
    'author',
    'created_utc',
    'url',
    'over_18',
    'edited',
    'spoiler',
    'stickied'
)