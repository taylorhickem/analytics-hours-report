import sys
import json
import boto3
import time
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import DataFrameWriter
from pyspark.sql.functions import col, concat_ws


PARAMETERS = [
    'JOB_NAME',
    'FILE_EXTENSION',
    'DB_NAME',
    'TABLE_NAMES',
    'TABLE_COLUMNS',
    'S3_BUCKET',
    'S3_PREFIX'
]


print('loading job parameters ...')
# glue job PARAMETERS
args = getResolvedOptions(sys.argv, PARAMETERS)
print(f'found parameters: {args}')
for a in args:
    globals()[a] = args[a]
    if a == 'TABLE_COLUMNS':
        globals()[a] = json.loads(args[a])

print('initializing glue context ...')
# Initialize Glue Context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(JOB_NAME, args)


S3_TMP_PATH = f's3://{S3_BUCKET}/{S3_PREFIX}tmp'


def save_table_to_csv(table_name):
    table_columns = TABLE_COLUMNS.get(table_name, {})
    
    # Read from the Glue catalog
    print(f'saving table {table_name} to CSV with columns {table_columns}...')

    print('creating dynamic frame ...')
    table = glueContext.create_dynamic_frame.from_catalog(
        database=DB_NAME, 
        table_name=table_name
    )

    if table_columns:    
        casting = [(k, f'cast:{v}') for k, v in table_columns.items()]
        table = table.resolveChoice(specs=casting)

    # Convert Glue DynamicFrame to Spark DataFrame
    df = table.toDF()
    
    print(f'writing dynamic frame to tmp path {S3_TMP_PATH}/{table_name} ...')

    # Write DataFrame to S3 in CSV format with .csv extension
    df.coalesce(1).write.mode('overwrite').option("header", "true").csv(f'{S3_TMP_PATH}/{table_name}')

table_names = TABLE_NAMES.split(',')

for t in table_names:
    save_table_to_csv(t)

job.commit()
