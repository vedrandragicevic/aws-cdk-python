import boto3
import botocore
import json
import logging
import sys

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logging.basicConfig(level=logging.INFO)

lake_formation_client = boto3.client("lakeformation")


def create_lf_tag(lf_tag):
    """ Creates LF Tags defined in the config json file. """
    try:
        response = lake_formation_client.create_lf_tag(
            TagKey=lf_tag['TagKey'],
            TagValues=lf_tag['TagValues']
        )
        logger.info(f"Successfully created LF tag: {lf_tag}.")
    except botocore.exceptions.ClientError as e:
        logger.error(f"Error occurred when creating LF tag: {e}.")


def register_data_lake_location(bucket_arn_list):
    """ Creates data lake location for given buckets. """
    for arn in bucket_arn_list:
        try:
            response = lake_formation_client.register_resource(
                ResourceArn=arn['arn'],
                UseServiceLinkedRole=True | False,
            )
            logger.info(f"Successfully registered location {arn['location']} in lake formation.")
        except botocore.exceptions.ClientError as e:
            logger.error(f"Error occurred when registering location {arn['location']} bucket: {e}.")
            continue


def add_lf_tag_to_resource(database_name_list, lf_tag):
    """ Adds LF tags to a given list of glue databases. """
    for database in database_name_list:
        try:
            response = lake_formation_client.add_lf_tags_to_resource(
                Resource={
                    'Database': {
                        'Name': database
                    },
                },
                LFTags=[
                    {
                        'TagKey': lf_tag['TagKey'],
                        'TagValues': lf_tag['TagValues']
                    },
                ]
            )
            logger.info(f"Successfully added {lf_tag['TagKey']=} on {database}.")
        except botocore.exceptions.ClientError as e:
            logger.error(f"Error adding lf tag key {lf_tag['TagKey']=} on {database} with error {e}.")
            continue


if __name__ == '__main__':
    logger.info('Starting boto3 deployment script...')

    env = sys.argv[1]
    config = json.load(open(f'lake_formation_service_config_{env}.json', 'r'))
    logger.info(f"Fetched config for '{env}' environment.")

    lf_tag_data = config.get('lf_tag_data')
    databases_list = config.get('databases')
    bucket_arns = config.get('bucket_arns')

    # Create LF tag
    create_lf_tag(lf_tag_data)

    # Register Data Lake Location
    register_data_lake_location(bucket_arn_list=bucket_arns)

    # Adding LF Tags to Databases defined in the config json file
    add_lf_tag_to_resource(database_name_list=databases_list, lf_tag=lf_tag_data)

    logger.info('Boto3 deployment completed!')
