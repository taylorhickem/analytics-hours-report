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
