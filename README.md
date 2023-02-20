# QueryStats

Figure out how your Athena queries are performing over time

## Overview

It can sometimes be difficult to figure out if you're running into concurrency limits with Athena or if you have queries scanning more data than expected.

This tool extracts your Athena execution history and uploads it back into S3 so you can analyze it with Athena. 😲

## Requirements

- boto3 library
- AWS cli configured
- An S3 bucket to store your results in

## Extracting Query History

```shell
python athena_stats.py <bucket> <prefix> <athena_table>

# e.g.:
python athena_stats.py aws-athena-query-results-eu-central-1-XXXXXXXXXXXXX path/to/tables/tables/athena_query_history data_lake_db.athena_query_history
```

The script will extract the past 45 days of your Athena history and upload the results in the bucket and prefix you specify.

Before running the script the first time, the Glue/Athena table definition must already exist, as this script always queries the `<athena_table>`
for extracting the latest watermark values -- i.e., the `max(status.submitted_at)` -- in order to resume from any existing data.

## Analyzing in Athena

### Preparing the tables

We use an opinionated table definition here, in order to work around reserved words of the original columns.
Furthermore, we use lower snake_case instead of CamelCased field names.

**Caution:**  
If you decide to use different column names, you may need to adjust the `WATERMARKS_QUERY_STRING` in the `athena_stats.py` script.

```sql
create external table <database>.athena_queries (
    query_id string
    , workgroup string
    , statement_type string
    , status struct<
        state:string
        , submitted_at:timestamp
        , completed_at:timestamp
    >
    , engine_version struct<
        selected_engine_version:string
        , effective_engine_version:string
    >
    , query_execution_context struct<
        database_name:string
    >
    , result_configuration struct<
        output_location:string
    >
    , result_reuse_configuration struct<
        result_reuse_by_age_configuration:struct<
            enabled:boolean
        >
    >
    , statistics struct<
        engine_execution_duration_ms:int
        , data_scanned_bytes:bigint
        , total_execution_duration_ms:bigint
        , query_queue_duration_ms:int
        , query_planning_duration_ms:int
        , service_processing_duration_ms:int
        , result_reuse_information:struct<
            reused_previous_result:boolean
        >
    >
    , query string
    , `_meta` struct<
        ingested_at:timestamp
        , ingestion_id:string
    >
)
--'partitioned by ( 
--    workgroup string
--    , dt string
--)'
row format serde 'org.openx.data.jsonserde.JsonSerDe'
with serdeproperties ( 
      'mapping.query_id'='QueryExecutionId'
    , 'mapping.statement_type'='StatementType'
    , 'mapping.submitted_at'='SubmissionDateTime'
    , 'mapping.completed_at'='CompletionDateTime'
    , 'mapping.engine_execution_duration_ms'='EngineExecutionTimeInMillis'
    , 'mapping.data_scanned_bytes'='DataScannedInBytes'
    , 'mapping.total_execution_duration_ms'='TotalExecutionTimeInMillis'
    , 'mapping.query_queue_duration_ms'='QueryQueueTimeInMillis'
    , 'mapping.query_planning_duration_ms'='QueryPlanningTimeInMillis'
    , 'mapping.service_processing_duration_ms'='ServiceProcessingTimeInMillis'
    , 'mapping.result_reuse_information'='ResultReuseInformation'
    , 'mapping.reused_previous_result'='ReusedPreviousResult'
    , 'mapping.workgroup'='WorkGroup'
    , 'mapping.engine_version'='EngineVersion'
    , 'mapping.selected_engine_version'='SelectedEngineVersion'
    , 'mapping.effective_engine_version'='EffectiveEngineVersion'
    , 'mapping.result_configuration'='ResultConfiguration'
    , 'mapping.output_location'='OutputLocation'
    , 'mapping.result_reuse_configuration'='ResultReuseConfiguration'
    , 'mapping.result_reuse_by_age_configuration'='ResultReuseByAgeConfiguration'
    , 'mapping.query_execution_context'='QueryExecutionContext'
    , 'mapping.database_name'='Database'
)
location 's3://<bucket>/<prefix>'
```

It may be beneficial to define another table on top of the same data which interprets the 
JSON lines as text. This provides a simple access to the uninterpreted table data,
which is useful for identifying any new fields that AWS may add in the future:

```sql
create external table <database>.athena_queries_json
    (
        v string
    )
--partitioned by ( 
    --workgroup string
    --dt string
--)
row format delimited
stored as textfile
location 's3://<bucket>/<prefix>'
tblproperties ('skip.header.line.count'='0')
```

### Example queries

Determine how many queries are running at any given second:

```sql
with

runtimes as (

    select
        query_id
        , status.state
        , status.submitted_at
        , status.completed_at
        , statistics.engine_execution_duration_ms
        , statistics.data_scanned_bytes
        , date_diff(
            'millisecond'
            , status.submitted_at
            , status.completed_at
        )                                                                       as runtime_diff
    from <database>.athena_queries
    where true
        and statement_type = 'DML'

)


, query_intervals as (

    select
        query_id
        , submitted_at
        , completed_at
        , sequence(
            date_trunc('second', submitted_at)
            , date_trunc('second', completed_at)
            , interval '1' second
        )                                                                       as intervals
    from runtimes

)


select
    t.interval_marker                                                           as occurred_at_sec
    , count(*)                                                                  as query_count
from query_intervals
cross join unnest(intervals) as t (interval_marker)
group by
    1
order by
    1 asc
```

## TODO

- [ ] Extract Athena API functionality into it's own class
  - [ ] Progress reporter based on current date / 45-day history
- [ ] Convert into Parquet on the fly
- [ ] Option to automatically create Athena table
- [ ] Fix table creation code to make use of the provided partition directory structure
