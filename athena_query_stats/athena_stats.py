import io
import gzip
import json
import argparse
import time
from datetime import datetime, date
from ulid import ulid
from json import JSONEncoder

import boto3

# This uses your default AWS profile settings
athena_client = boto3.client('athena')
s3_client = boto3.client('s3')

# 50 is the max query execution details we can fetch in a batch
MAX_ATHENA_BATCH_SIZE = 50
UPLOAD_TARGET_BATCH_SIZE = 5000

# Settings for querying the watermarks of existing data.
# Note: it's tempting to apply some partition pruning clause here, but this would be 
# dangerous with workgroups that haven't been in use recently. Here we'd then fall back to 
# fetching all data again.\
# Thus, pruning would only be safe on a time range greater or equal the max history that 
# the API contains data. And then it's of no great use...
WATERMARKS_QUERY_STRING = '''
    select
        workgroup
        , to_unixtime(
            max(status.submitted_at)
        )                                                                           as submitted_at_max
    from $ATHENA_TABLE$
    group by 
        workgroup
'''

RESULTS_FETCH_MAX_RETRY = 10
RESULTS_FETCH_RETRY_WAIT_SEC = 10



def parse_args():
    """Command line argument parser"""
    parser = argparse.ArgumentParser(description='Extract Athena query execution data to S3.')
    parser.add_argument("bucket", help="The S3 bucket name to upload query executions to")
    parser.add_argument("prefix", help="The prefix in which to store uploaded query executions")
    parser.add_argument("athena_table", help="The (qualified) Athena table name where to look up the watermarks of previous runs")
    return parser.parse_args()


def loop_and_fetch_stats(bucket, prefix, athena_table):
    execution_ids = []
    counter = 1
    watermarks = get_watermarks(athena_table)
    ingestion_id = ulid().lower()
    for workgroup in get_workgroups():
        print(f"Processing workgroup {workgroup}...")
        watermark = watermarks.get(workgroup, 0)
        print(f"Fetching queries submitted after epoch timestamp {watermark}...")
        upload_batch = []
        ingested_at = datetime.now()
        for execution_ids in get_execution_ids(workgroup):
            print("Fetching data on batch %d (%d total queries)" % (counter, counter * MAX_ATHENA_BATCH_SIZE))
            if len(execution_ids) > 0:
                stats = get_query_executions(execution_ids)
                # Filter stats so it only contains new data
                filtered_stats = filter_query_executions(stats, watermark)
            else:
                filtered_stats = []
            
            if len(filtered_stats) > 0:
                counter += 1
                upload_batch.extend(filtered_stats)
                if len(upload_batch) >= UPLOAD_TARGET_BATCH_SIZE:
                    upload_to_s3(upload_batch, bucket, prefix, athena_table, ingested_at, ingestion_id)
                    # initialize next batch:
                    upload_batch = []
                    ingested_at = datetime.now()
            else:
                # Upload the remaining batch which did not reach the UPLOAD_TARGET_BATCH_SIZE
                if len(upload_batch) > 0:
                    upload_to_s3(upload_batch, bucket, prefix, athena_table, ingested_at, ingestion_id)
                print(f"All recent queries processed.")
                break


def get_watermarks(athena_table):
    print("Fetching watermarks of the query submitted timestamps per workgroup...")
    # FIXME: This is not bullet-proof, as we use no pagination at the moment...
    query_string = WATERMARKS_QUERY_STRING.replace('$ATHENA_TABLE$', athena_table )
    query = athena_client.start_query_execution(QueryString=query_string)

    retry_count = 0
    while retry_count <= RESULTS_FETCH_MAX_RETRY:
        try:
            response = athena_client.get_query_results(
                QueryExecutionId=query.get('QueryExecutionId'),
                MaxResults=1000
            )
            print("Success.")
            break
        except:
            retry_count += 1
            print(f"Waiting {RESULTS_FETCH_RETRY_WAIT_SEC} sec for query to succeed...")
            time.sleep(RESULTS_FETCH_RETRY_WAIT_SEC)
            print(f"Retry ({retry_count}/{RESULTS_FETCH_MAX_RETRY})...")

    watermarks = {row.get("Data")[0].get("VarCharValue"): float(row.get("Data")[1].get("VarCharValue"))  for row in response.get("ResultSet").get("Rows")[1:]}
    return watermarks

def upload_to_s3(upload_batch, bucket, prefix, athena_table, ingested_at):
    date_partition = (
        ingested_at.strftime("year=%Y/")
        + ingested_at.strftime("month=%m/")
        + ingested_at.strftime("day=%d/")
        + ingested_at.strftime("dt=%Y-%m-%d")
    )
    # We'll do this all in memory
    # Create a gzip'ed JSON bytes object
    # Useful :) https://gist.github.com/veselosky/9427faa38cee75cd8e27
    writer = io.BytesIO()
    gzip_out = gzip.GzipFile(fileobj=writer, mode='w')
    for record in upload_batch:
        # print(json.dumps(record, default=json_serial))
        # json.dump(record, gzip_out, default=json_serial)
        record['_meta'] = {
            "ingested_at": ingested_at.timestamp(),
            "ingestion_id": ingestion_id
        }
        json_line = json.dumps(record, default=json_serial) + "\n"
        gzip_out.write(json_line.encode())
    gzip_out.close()

    key = prefix + '/' + date_partition + '/' + ulid().lower() + ".json.gz"
    print(f"Uploading batch to location {key}...")
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        ContentType='text/plain',  # the original type
        ContentEncoding='gzip',  # MUST have or browsers will error
        Body=writer.getvalue()
    )
    print("Done.")
    add_table_partition(athena_table, date_partition)


def add_table_partition(athena_table, date_partition):
    print("Registering Athena table partition for this ingestion date...")
    query_string = f'''
        alter table {athena_table}_json add if not exists
            partition (dt='{date_partition}')
    '''
    query = athena_client.start_query_execution(QueryString=query_string)
    time.sleep(3)

    retry_count = 0
    while retry_count <= RESULTS_FETCH_MAX_RETRY:
        try:
            response = athena_client.get_query_results(
                QueryExecutionId=query.get('QueryExecutionId'),
                MaxResults=1000
            )
            print("Success.")
            break
        except:
            retry_count += 1
            print(f"Waiting {RESULTS_FETCH_RETRY_WAIT_SEC} sec for query to succeed...")
            time.sleep(RESULTS_FETCH_RETRY_WAIT_SEC)
            print(f"Retry ({retry_count}/{RESULTS_FETCH_MAX_RETRY})...")
    status_code = response.get('ResponseMetadata').get('HTTPStatusCode')
    if status_code == 200:
        print("Success.")
    else:
        print(response)
        raise Exception('Failed registering Athena partition!')


def get_query_executions(ids):
    """Retrieve details on the provided query execuution IDs"""
    response = athena_client.batch_get_query_execution(
        QueryExecutionIds=ids
    )
    return response['QueryExecutions']


def get_execution_ids(workgroup):
    """Retrieve the list of all executions from the Athena API"""
    query_params = {}  # Empty dictionary for next token
    while True:
        response = athena_client.list_query_executions(WorkGroup=workgroup, **query_params)
        yield response['QueryExecutionIds']

        if 'NextToken' in response:
            query_params['NextToken'] = response['NextToken']
        else:
            break

def filter_query_executions(stats, watermark):
    filtered_queries = [
        q for q in stats if q.get("Status").get("SubmissionDateTime").timestamp() > watermark
    ]
    return filtered_queries

def get_workgroups():
    """Retrieve Athena workgroups"""
    # FIXME: This is not bullet-proof, as we use no pagination at the moment...
    response = athena_client.list_work_groups()
    workgroups = [i.get('Name') for i in response.get('WorkGroups')]
    return workgroups

def json_serial(obj):
    """JSON serializer for datetime objects: use epoch seconds, so we can directly use it in Athena"""
    if isinstance(obj, (datetime, date)):
        return obj.timestamp()
    raise TypeError("Type %s not serializable" % type(obj))

# entry point:
def run():
    args = parse_args()
    loop_and_fetch_stats(args.bucket, args.prefix, args.athena_table)


if __name__ == "__main__":
    run()
