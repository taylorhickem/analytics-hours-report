# Hours report analytics pipeline
Create hours report tables and visualizations from raw sources in AWS.

## Source database

_tgevents_
```
 |-- timestamp: string (nullable = true)
 |-- DOW: long (nullable = true)
 |-- year: long (nullable = true)
 |-- last_modified: string (nullable = true)
 |-- week: long (nullable = true)
 |-- date: string (nullable = true)
 |-- comment: string (nullable = true)
 |-- duration_hrs: double (nullable = true)
 |-- time: string (nullable = true)
 |-- activity: string (nullable = true)
 |-- month: long (nullable = true)
```

## S3 location
the S3 locations are organized by role. Each role has it's own bucket. The location is the same for all roles

`95_data_analytics/staged/<table_name>.csv`


## pipeline steps

1. run glue PySpark job to save DynamoDB table to csv using Dynamic_Frame in tmp location
2. use Lambda to rename file <table_name>.csv in the staged directory
