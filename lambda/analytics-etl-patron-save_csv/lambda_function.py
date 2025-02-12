"""
analytis-etl-patron-save_csv
moves and renames csv files in analytics folder
runs after PySpark Glue job that creates the csv from DynamoDB tables source
"""

import os
import json
import boto3


clients = {
    's3': None
}

S3_BUCKET = ''
S3_PREFIX = ''
FILE_EXTENSION = ''

PARAMETERS = [
    'S3_BUCKET',
    'S3_PREFIX',
    'FILE_EXTENSION'
]


def lambda_handler(event, context):
    load_env_parameters()

    client_load('s3')
    tmp_dir = f'{S3_PREFIX}/tmp'
    tagged_keys = move_tagged_files(S3_BUCKET, tmp_dir, S3_PREFIX, file_extension=FILE_EXTENSION)
    client_unload('s3')
    success = all([tagged_keys[k]['success'] for k in tagged_keys]) if tagged_keys else False
    if success:
        table_names = list(tagged_keys.keys())
        message = f'SUCCESS. created csv files for tables {table_names}'
    else:
        message = 'ERROR. failed to create csv files for at least one table.'

    response = {
        'statusCode': 200,
        'message': message,
        'data': tagged_keys
    }
    
    return response


def load_env_parameters():
    params = {}
    for a in PARAMETERS:
        if a in os.environ:
            env_param = os.environ[a]
            globals()[a] = env_param
            params[a] = env_param
    if params:
        print(f'env parameters: {params}')
    else:
        print('no env parameters found')


def client_load(service):
    global clients
    if clients[service] is None:
        clients[service] = boto3.client(service)
        

def client_unload(service):
    global clients
    if clients[service] is not None:
        clients[service] = None


def move_tagged_files(s3_bucket, source_dir, target_dir, file_extension='.csv'):
    tagged_keys = get_tagged_keys(s3_bucket, source_dir, file_extension)
    if tagged_keys:
        keys = list(tagged_keys.keys())
        for k in keys:
            target_key = f'{target_dir}/{k}.{file_extension}'
            source_key = tagged_keys[k]['source']
            copy_source = {'Bucket': s3_bucket, 'Key': source_key}
            clients['s3'].copy_object(
                CopySource=copy_source, 
                Bucket=s3_bucket, 
                Key=target_key
            )
            clients['s3'].delete_object(Bucket=s3_bucket, Key=source_key)
            tagged_keys[k]['target'] = target_key
            tagged_keys[k]['success'] = True
    return tagged_keys


def get_tagged_keys(s3_bucket, directory, file_extension):
    tagged_keys = {}
    dirs, keys = s3_dir_list(s3_bucket, directory)
    if dirs:
        table_names = get_subfolders(directory, dirs)
        tagged_keys = {t: {'source': [k for k in keys if (k.endswith(file_extension) and t in k)][0]} for t in table_names}
    return tagged_keys


def get_subfolders(parent, keys):
    subfolders = []
    if keys:
        subfolders = [f.replace(f'{parent}/', '') for f in keys if not f == parent]
    return subfolders


def s3_dir_list(s3_bucket, directory):
    keys = []
    dirs = []
    response = clients['s3'].list_objects_v2(
        Bucket=s3_bucket, 
        Prefix=directory
    )
    contents = response.get('Contents', [])
    if contents:
        keys = [c['Key'] for c in contents]
        if keys:
            path_tuples = [os.path.split(k) for k in keys]
            dirs = list(set([p[0] for p in path_tuples if p[0]]))

    return dirs, keys