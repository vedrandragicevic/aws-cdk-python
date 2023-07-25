from aws_cdk import (
    # Duration,
    Stack,
    Tags,
    # aws_sqs as sqs,
    aws_lambda,
    aws_iam,
    aws_dynamodb as dynamodb,
    RemovalPolicy
)
from constructs import Construct
from types import SimpleNamespace
from dataclasses import dataclass


@dataclass
class DynamoStack(Stack):

    dynamo_config_table: dynamodb.Table

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

    def build_applications_stack(self, context: SimpleNamespace):
        """ Main method to control construction of Dynamo stack. """
        self.build_dynamo_table(context, table_name="dynamo-config-vex",
                                partition_key="name")

        return self.dynamo_config_table

    def build_dynamo_table(self, context: SimpleNamespace, table_name: str, partition_key: str):
        """ Method builds DynamoDB Table. """
        self.dynamo_config_table = dynamodb.Table(self, "DynamoDBConfigTable",
                                                  partition_key=dynamodb.Attribute(
                                                      name=partition_key, type=dynamodb.AttributeType.STRING),
                                                  billing_mode=dynamodb.BillingMode.PROVISIONED,
                                                  table_name=f"{context.environment}-{table_name}",
                                                  removal_policy=RemovalPolicy.DESTROY
                                                  )
