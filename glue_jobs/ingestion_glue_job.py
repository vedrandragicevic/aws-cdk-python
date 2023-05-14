import os
import io
import sys
import pytz
import json
import time
import struct
import boto3
import logging
import pandas as pd
import awswrangler as wr
import datetime as dt
from pathlib import Path
from simpledbf import Dbf5
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv,
                          [
                              'CONTROL_JOB',
                              'GLOBAL_VARIABLES_TABLE',
                              'CONTROL_JOB_TABLE',
                              'CONTROL_JOB_STEP_TABLE'
                          ]
                          )

stdout_handler = logging.StreamHandler(sys.stdout)
logger = logging.getLogger(args['CONTROL_JOB'])
logger.addHandler(stdout_handler)
logger.setLevel(logging.INFO)

DynamoDB = boto3.resource('dynamodb', endpoint_url='https://dynamodb.us-east-1.amazonaws.com')

global_variables_table = args["GLOBAL_VARIABLES_TABLE"]
control_job_table = args["CONTROL_JOB_TABLE"]
control_step_table = args["CONTROL_JOB_STEP_TABLE"]

# Instantiate configuration
config = {
    'job_name': args.get('CONTROL_JOB'),

    's3_source_bucket': str(),
    's3_source_bucket_key': str(),

    's3_target_bucket': str(),
    's3_target_bucket_key': str(),
    's3_processed_bucket_key': str(),

    'database_name': str(),

    'file_rename_desc': dict(),
    'replace_chars': dict(),

    'start_step': 1,
    'end_step': -1
}


# Populate configuration
def get_job_arguments():
    global control_job_table
    job_get_item_kwargs = {'Key': {'name': config['job_name']}}

    response = control_job_table.get_item(**job_get_item_kwargs)
    job_record = response['Item']

    if not job_record['active']:
        logger.info("[WARNING][JOB] Control job table record: '{}' is inactive".format(config['job_name']))
    else:
        return json.loads(job_record['arguments'])


def get_global_vars(name):
    global global_variables_table
    get_item_kwargs = {'Key': {'name': name}}

    response = global_variables_table.get_item(**get_item_kwargs)
    return response['Item']


def setup_job_config(args):
    global config

    global_variables_src_record = get_global_vars(args['s3_inbound_bucket']['global_var_name'])
    global_variables_tgt_record = get_global_vars(args['s3_data_lake_bucket']['global_var_name'])

    config['s3_source_bucket'] = global_variables_src_record['value'].replace("s3://", "").replace("S3://", "")
    config['s3_source_bucket_key'] = args['s3_inbound_key']

    config['s3_target_bucket'] = global_variables_tgt_record['value'].replace("s3://", "").replace("S3://", "")
    config['s3_target_bucket_key'] = args['s3_data_lake_key']
    config['s3_processed_bucket_key'] = args['s3_inbound_processed_key']

    config['database_name'] = args['database_name']
    config['file_rename_desc'] = args['file_name_2_columns']
    config['replace_chars'] = args['replace_chars']
    config['start_step'] = args['start_step']
    config['end_step'] = args['end_step']

    config['threshold_mb'] = args.get('threshold_mb', 80)
    config['threshold_file_count'] = args.get('threshold_file_count', 500)
    config['s3_rejected_bucket_key'] = args.get('s3_inbound_rejected_key', 'rejected/unknown/')
    config['s3_corrupted_bucket_key'] = args.get('s3_corrupted_bucket_key', 'corrupt/unknown/')


def get_step(step_name):
    global control_step_table
    step_get_item_kwargs = {'Key': {'name': step_name}}

    response = control_step_table.get_item(**step_get_item_kwargs)

    try:
        step_record = response['Item']
    except KeyError:
        # Returns None so the while loop stops
        logger.info("[WARNING][STEP] Step '{}' doesn't exist".format(step_name))
        return None

    if step_record['active']:
        # Returns dict so the step is processed
        return step_record
    else:
        # Returns False so the while loop continues
        logger.info("[WARNING][STEP] Control step record: '{}' is inactive".format(step_name))
        return False


# Create EDH columns and append them to existing DataFrames
def get_edh_columns(posix_filepath):
    global config
    start_columns = list()
    end_columns = list()
    split_file_name = posix_filepath.stem.split("_")

    for col_name, ordering in config['file_rename_desc'].items():
        if ordering['lake_order'] > 0:
            start_column = {
                col_name: split_file_name[ordering['file_order']],
                'index': ordering['lake_order']
            }
            start_columns.append(start_column)
        else:
            end_column = {
                col_name: split_file_name[ordering['file_order']],
                'index': ordering['lake_order']
            }
            end_columns.append(end_column)

    start_columns = sorted(start_columns, key=lambda x: x['index'])
    end_columns = sorted(end_columns, key=lambda x: x['index'])
    return start_columns, end_columns


def set_edh_columns(df, start_columns, end_columns):
    global config
    cols = df.columns.tolist()

    for new_column in start_columns:
        index = new_column.pop('index')  # Removes the index so only one item remains
        new_col, col_value = new_column.popitem()  # Pops the last item in the dictionary

        df[new_col] = col_value
        # append new columns to the DataFrame beginning
        cols.insert(index - 1, new_col)

    for new_column in end_columns:
        index = new_column.pop('index')  # Removes the index so only one item remains
        new_col, col_value = new_column.popitem()  # Pops the last item in the dictionary

        df[new_col] = col_value
        # append new columns to the DataFrame end
        cols.insert(len(cols) + index + 1, new_col)
    return rename_df_columns(df[cols])


def rename_df_columns(df):
    columns = dict()
    for col in df.columns.tolist():
        renamed_col = col
        for key, value in config['replace_chars'].items():
            if key == '?' and key in renamed_col:
                renamed_col = 'is_' + renamed_col.replace('?', '')
            else:
                renamed_col = renamed_col.replace(key, value)
        columns.update({col: renamed_col.lower()})
    return df.rename(columns=columns)


## Filter files by partitions
def get_inbound_partitions(table_name):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(config['s3_source_bucket'])
    prefix = os.path.join(config['s3_source_bucket_key'], table_name + "/")
    inbound_file_objects = list(bucket.objects.filter(Prefix=prefix))

    delete_objs = [file_obj for file_obj in inbound_file_objects if file_obj.key.endswith("/")]
    for file_obj in delete_objs:
        inbound_file_objects.remove(file_obj)

    for file_obj in inbound_file_objects:
        posix_filepath = Path(file_obj.key)
        inbound_dttm = posix_filepath.stem.split("_")[-1]
        file_obj.inbound_dttm = inbound_dttm

    sorted_inbound_file_objects = sorted(inbound_file_objects, key=lambda x: x.inbound_dttm)
    inbound_partitions = {}
    for file_obj in sorted_inbound_file_objects:
        if file_obj.inbound_dttm in inbound_partitions.keys():
            inbound_partitions[file_obj.inbound_dttm].append(file_obj)
        else:
            inbound_partitions[file_obj.inbound_dttm] = [file_obj]
    return inbound_partitions


def _get_partition_thresholds(partition):
    size_in_mb = 0
    for file_obj in partition:
        size_in_B = file_obj.size
        size_in_mb += size_in_B / 1.e6
    return size_in_mb, len(partition)


def get_threshold_partitions(inbound_partitions):
    size_in_mb = 0
    inbound_threshold_partitions = []
    threshold_partitions = []
    i = 1
    for inbound_dttm, inbound_partition in inbound_partitions.items():
        partition_mb, partition_file_count = _get_partition_thresholds(inbound_partition)
        if ((size_in_mb + partition_mb) >= config['threshold_mb']
                or ((i + partition_file_count) >= config['threshold_file_count'])):
            inbound_threshold_partitions.append(threshold_partitions)
            threshold_partitions = []
            size_in_mb = 0
            i = 1

        for file_obj in inbound_partition:
            size_in_B = file_obj.size
            size_in_mb += size_in_B / 1.e6

            threshold_partitions.append(
                os.path.join('s3://', config['s3_source_bucket'], file_obj.key)
            )
            i += 1

    if len(threshold_partitions) > 0:
        inbound_threshold_partitions.append(threshold_partitions)
    return inbound_threshold_partitions


def get_filtered_file_lists(table_name):
    inbound_partitions = get_inbound_partitions(table_name)
    inbound_threshold_partitions = get_threshold_partitions(inbound_partitions)
    return inbound_threshold_partitions


def get_dfs_from_s3(step_config, files_to_load):
    rejected_files = []
    corrupt_files = []
    df_list = []

    for source_file_path in files_to_load:
        posix_filepath = Path(source_file_path)

        if step_config['file_type'] == 'csv':
            df = wr.s3.read_csv(source_file_path, dtype=object)
        elif step_config['file_type'] == 'dbf':
            buffer = io.BytesIO()
            wr.s3.download(path=source_file_path, local_file=buffer)
            buffer.seek(0)

            try:
                dbf = Dbf5(buffer, codec='latin-1')
                df = dbf.to_dataframe(dtype=object)
            except AssertionError:
                logger.info("[ERROR] Corrupt (non-recoverable) file: {}".format(source_file_path))
                corrupt_files.append(source_file_path)
                df = None
            except struct.error:
                logger.info("[ERROR] Rejected (recoverable) file: {}".format(source_file_path))
                rejected_files.append(source_file_path)
                df = None

        else:
            df = None

        if df is not None:
            start, end = get_edh_columns(posix_filepath)
            df = set_edh_columns(df, start, end)
            df_list.append(df)
        else:
            logger.info("[EMPTY] File: {}".format(source_file_path))

    return rejected_files, corrupt_files, df_list


def move_files_to_processed(step_config, files_to_load, rejected=False, corrupt=False):
    global config
    processed_files = list()
    now = dt.datetime.now(pytz.UTC)
    s3 = boto3.client('s3')

    if rejected:
        status = "REJECTED"
    else:
        status = "PROCESSED"

    for source_file_path in files_to_load:
        posix_filepath = Path(source_file_path)
        if not rejected and not corrupt:
            processed_bucket_key = os.path.join(
                config['s3_processed_bucket_key'],
                step_config['table_name'],
                now.strftime('%Y/%m/%d'),
                posix_filepath.name
            )
        if rejected:
            processed_bucket_key = os.path.join(
                config['s3_rejected_bucket_key'],
                step_config['table_name'],
                now.strftime('%Y/%m/%d'),
                posix_filepath.name
            )
        if corrupt:
            processed_bucket_key = os.path.join(
                config['s3_corrupted_bucket_key'],
                step_config['table_name'],
                now.strftime('%Y/%m/%d'),
                posix_filepath.name
            )

        data_source = {
            'Bucket': config['s3_source_bucket'],
            'Key': os.path.join(config['s3_source_bucket_key'], step_config['table_name'], posix_filepath.name)
        }
        s3.copy(data_source, config['s3_source_bucket'], processed_bucket_key)
        s3.delete_object(**data_source)

        processed_files.append(posix_filepath.name)
        logger.info("[{}] {} ==> {}".format(
            status,
            os.path.join("s3://", config['s3_source_bucket'], config['s3_source_bucket_key'], step_config['table_name'],
                         posix_filepath.name),
            os.path.join("s3://", config['s3_source_bucket'], processed_bucket_key)
        ))
    return processed_files


## Ingest inbound files
def ingest_objects(step_config, filtered_file_lists):
    target_path = os.path.join("s3://", config['s3_target_bucket'], config['s3_target_bucket_key'],
                               step_config['table_name'].lower())
    logger.info("[STEP] {} : Ingestion initiated, target path: {}".format(step_config['step_name'], target_path))

    now = dt.datetime.now(pytz.UTC)
    processed_dttm = now.strftime('%Y%m%d%H%M%S')
    processed_files = list()
    for i, files_to_load in enumerate(filtered_file_lists):
        now = dt.datetime.now(pytz.UTC)
        if processed_dttm == now.strftime('%Y%m%d%H%M%S'):
            time.sleep(1)
        processed_dttm = now.strftime('%Y%m%d%H%M%S')

        logger.info(
            "[STEP] {} : Processing {} of {} batches ({} MB threshold, {} file count threshold), edh_processed_dttm: {}".format(
                step_config['step_name'], i + 1, len(filtered_file_lists),
                config['threshold_mb'], config['threshold_file_count'], processed_dttm
            ))
        logger.info("[STEP][BATCH#{}]: {}".format(i + 1, json.dumps(files_to_load, indent=2)))

        rejected_files, corrupt_files, df_list = get_dfs_from_s3(step_config, files_to_load)

        try:
            df = pd.concat(df_list)
        except ValueError:
            df = pd.DataFrame()

        if not df.empty:
            df['edh_processed_dttm_utc'] = processed_dttm
            null_cells = df.isnull()

            wr.s3.to_parquet(
                df=df.mask(null_cells, None),
                path=target_path,
                dataset=True,
                partition_cols=['edh_processed_dttm_utc'],
                database=config['database_name'],
                table=step_config['table_name'].lower(),
                dtype={col: 'string' for col in df.columns}
            )

            logger.info("[STEP] {} : [DONE - LAKE] {} ==> {}".format(
                step_config['step_name'],
                os.path.join("s3://", config['s3_source_bucket'], config['s3_source_bucket_key'],
                             step_config['table_name']),
                os.path.join(target_path, 'edh_processed_dttm_utc=' + processed_dttm)
            ))

        else:
            logger.info("[STEP][EMPTY] Table : {}".format(step_config['table_name']))

        rejected = None
        corrupt = None
        if rejected_files:
            for rejected in rejected_files:
                files_to_load.remove(rejected)
            rejected = move_files_to_processed(step_config, rejected_files, rejected=True)
        if corrupt_files:
            for corrupt in corrupt_files:
                files_to_load.remove(corrupt)
            corrupt = move_files_to_processed(step_config, corrupt_files, corrupt=True)
        processed = move_files_to_processed(step_config, files_to_load, rejected=False)
        if i == len(filtered_file_lists) - 1:
            if rejected:
                processed += rejected
            if corrupt:
                processed += corrupt
            processed_files.append(processed)
    return processed_files


def write_last_extract_day(step_name, processed_files):
    global control_step_table
    now = dt.datetime.now(pytz.UTC)

    response = control_step_table.update_item(
        Key={'name': step_name},
        UpdateExpression="set last_successful_load=:f, last_load_dttm_utc=:d",
        ExpressionAttributeValues={
            ':f': json.dumps({'processed_files': processed_files}, indent=2),
            ':d': now.strftime('%Y%m%d%H%M%S')
        },
        ReturnValues="UPDATED_NEW"
    )

    logger.info("[STEP][COMPLETED] {}: {}".format(step_name, json.dumps(response, indent=2)))


# Run steps
def process_step(step_record):
    args = json.loads(step_record['arguments'])
    step_config = {
        'step_name': step_record['name'],
        'table_name': args['name'],
        'file_type': args['file_type'],
        'last_successful_load': dict()
    }

    if 'last_successful_load' in step_record.keys() and step_record['last_successful_load']:
        step_config['last_successful_load'] = json.loads(step_record['last_successful_load'])

    logger.info("[STEP] {} : Table name: {}".format(step_config['step_name'], step_config['table_name']))
    filtered_file_lists = get_filtered_file_lists(step_config['table_name'])

    if filtered_file_lists:
        successful_load = ingest_objects(step_config, filtered_file_lists)
        write_last_extract_day(step_config['step_name'], successful_load)
    else:
        inbound_path = os.path.join(
            "s3://",
            config['s3_source_bucket'],
            config['s3_source_bucket_key'],
            step_config['table_name'] + '/'
        )
        logger.info("[STEP][EMPTY] No files to read at '{}'".format(inbound_path))


def main():
    if config['job_name'] == "update-default":
        logger.info("[EXIT] Job name invalid. Got '{}'".format(config['job_name']))
        return

    # Setup configuration - fetch parameters from the Control Job table
    job_arguments = get_job_arguments()
    setup_job_config(job_arguments)

    logger.info("[INFO] Job name: {}".format(config['job_name']))
    logger.info("[INFO] Configuration:\n{}".format(json.dumps(config, indent=3)))

    start_step = config['start_step']
    end_step = config['end_step']

    if end_step == -1:
        step_record = True
        step_number = start_step
        while step_record is not None:
            step_name = config['job_name'] + '-' + str(step_number)
            step_record = get_step(step_name)
            if step_record:
                process_step(step_record)
            step_number += 1
    else:
        for step_number in range(start_step, end_step + 1):
            step_name = config['job_name'] + '-' + str(step_number)
            step_record = get_step(step_name)
            if step_record:
                process_step(step_record)


if __name__ == '__main__':
    main()