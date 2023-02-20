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

# Settings for querying the watermarks of existing data:
# FIXME: pass table name as input param.
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
    for workgroup in get_workgroups():
        print(f"Processing workgroup {workgroup}...")
        watermark = watermarks.get(workgroup, 0)
        print(f"Fetching queries submitted after epoch timestamp {watermark}...")
        upload_batch = []
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
                    upload_to_s3(upload_batch, bucket, prefix, workgroup)
                    upload_batch = []
            else:
                # Upload the remaining batch which did not reach the UPLOAD_TARGET_BATCH_SIZE
                if len(upload_batch) > 0:
                    upload_to_s3(upload_batch, bucket, prefix, workgroup)
                print(f"All recent queries processed for workgroup {workgroup}")
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

def upload_to_s3(upload_batch, bucket, prefix, workgroup):
    # We'll do this all in memory
    # Create a gzip'ed JSON bytes object
    # Useful :) https://gist.github.com/veselosky/9427faa38cee75cd8e27
    writer = io.BytesIO()
    gzip_out = gzip.GzipFile(fileobj=writer, mode='w')
    now = datetime.now()
    for record in upload_batch:
        # print(json.dumps(record, default=json_serial))
        # json.dump(record, gzip_out, default=json_serial)
        record['_meta'] = {
            "ingested_at": now.timestamp(),
            "ingestion_id": ingestion_id  # global variable
        }
        json_line = json.dumps(record, default=json_serial) + "\n"
        gzip_out.write(json_line.encode())
    gzip_out.close()

    key = prefix + '/workgroup=' + workgroup + '/' + now.strftime("dt=%Y-%m-%d") + '/' + ulid().lower() + ".json.gz"
    print(f"Uploading batch to location {key}...")
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        ContentType='text/plain',  # the original type
        ContentEncoding='gzip',  # MUST have or browsers will error
        Body=writer.getvalue()
    )
    print("Done.")


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


if __name__ == "__main__":
    args = parse_args()
    ingestion_id = ulid().lower()
    loop_and_fetch_stats(args.bucket, args.prefix, args.athena_table)
