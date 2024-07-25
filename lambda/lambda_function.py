"""analytics-etl-patron-hours_report
"""
import json
import pandas as pd
import datetime as dt
import boto3
from pyathena import connect


DATASOURCE = 'patronigence'
DB_NAME = 'default'
TABLE_NAME = 'tgevents'
TARGET_BUCKET = 'taylorhickem-patron'
TARGET_PREFIX = '95_data_analytics/staged'


def lambda_handler(event, context):
    print(f'event parameters: {event}')
    
    
    
    response = {
        'statusCode': 200,
        'message': 'script ran without exception'
    }
    
    return response