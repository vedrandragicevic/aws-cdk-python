"""
LAMBDA - S3 UPLOAD TRIGGER - SNS - DYNAMODB - S3 BUCKET - LAMBDA LAYERS
"""

import json
from re import I
from aws_cdk import (
    Tags,
    Duration,
    Stack,
    aws_ec2 as ec2,
    aws_dynamodb as dynamodb,
    aws_iam,
    aws_lambda,
    aws_iam,
    aws_events,
    RemovalPolicy,
    aws_events_targets,
    aws_logs,
    aws_s3 as s3,
    aws_sns,
    aws_sns_subscriptions,
    aws_cloudwatch,
    aws_cloudwatch_actions,
    aws_ssm,
    aws_glue as glue,
    aws_glue_alpha as glue_alpha,
    aws_lakeformation as lakeformation,

)
from constructs import Construct
from types import SimpleNamespace
import uuid


class PythonProjectStack(Stack):
    """
    # Initiate a class when using context from cdk.json

    def __init__(self, scope: Construct, construct_id: str, context: SimpleNamespace, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)


        Example bucket creation:
        ingestion_bucket = s3.Bucket(self, "DataIngestionBucket",
                                    bucket_name=f"{context.environment}-data-ingestion",
                                    encryption=s3.BucketEncryption.S3_MANAGED,
                                    removal_policy=RemovalPolicy.DESTROY
                                    )
    """

    def __init__(self, scope: Construct, construct_id: str) -> None:
        super().__init__(scope, construct_id)

        # Importing VPC
        # main_vpc = ec2.Vpc.from_lookup(self, "Vpc", vpc_id="12345")

        # SNS TOPICS FOR SUCCESS AND FAILURE
        sns_success_topic = aws_sns.Topic(
            self,
            id="SNSSuccessTopic",
            topic_name=f"{context.environment}-file-uploads-success",
        )

        sns_failure_topic = aws_sns.Topic(
            self,
            id="SNSFailureTopic",
            topic_name=f"{context.environment}-file-uploads-failure",
        )

        # CREATE REQUIRED SUCCESS SUBSCRIPTION FOR 'alarm_notification_emails' LIST IN cdk.json
        for email in context.alarm_notification_emails:
            sub_id = (
                    email.rsplit("@", 1)[0]
                    + "_"
                    + email.rsplit("@", 1)[1].rsplit(".", 1)[0]
            ).replace(".", "_")
            sub_id = (
                    "".join(word.title() for word in sub_id.split("_"))
                    + context.sub_id_suffix_success
            )

            # One subscription per email address
            subscription = aws_sns.Subscription(
                self,
                id=sub_id,
                topic=sns_success_topic,
                endpoint=email,
                protocol=aws_sns.SubscriptionProtocol.EMAIL,
            )

        # CREATE REQUIRED FAILURE SUBSCRIPTION FOR 'alarm_notification_emails' LIST IN cdk.json
        for email in context.alarm_notification_emails:
            sub_id = (
                    email.rsplit("@", 1)[0]
                    + "_"
                    + email.rsplit("@", 1)[1].rsplit(".", 1)[0]
            ).replace(".", "_")
            sub_id = (
                    "".join(word.title() for word in sub_id.split("_"))
                    + context.sub_id_suffix_failure
            )

            # One subscription per email address
            subscription = aws_sns.Subscription(
                self,
                id=sub_id,
                topic=sns_failure_topic,
                endpoint=email,
                protocol=aws_sns.SubscriptionProtocol.EMAIL,
            )

        # S3 Bucket Construct
        s3_bucket = s3.Bucket(self, "S3Bucket",
                                          bucket_name=f"{context.environment}-file-uploads",
                                          encryption=s3.BucketEncryption.S3_MANAGED,
                                          removal_policy=RemovalPolicy.DESTROY
                                          )

        # Glue Database Construct
        file_uploads_glue_database = glue_alpha.Database(
            self,
            "GlueDB",
            database_name=f"file_uploads",
            location_uri=f"s3://{data_lake_bucket.bucket_name}/D1/subfolder/")
        file_uploads_glue_database.apply_removal_policy(RemovalPolicy.DESTROY)

        # DynamoDB Table Construct
        dynamo_table = dynamodb.Table(self, "DynamoDBConfigTable",
                                              partition_key=dynamodb.Attribute(
                                                  name="path", type=dynamodb.AttributeType.STRING),
                                              billing_mode=dynamodb.BillingMode.PROVISIONED,
                                              table_name=f"{context.environment}-dynamo-config",
                                              removal_policy=RemovalPolicy.DESTROY
                                              )

        # Lambda Layer AWS WRANGLER Construct
        awswrangler_layer = aws_lambda.LayerVersion(
            self,
            "AWSWranglerLambdaLayer",
            description="Lambda Layer containing AWS Wrangler library",
            code=aws_lambda.Code.from_asset("apps/lambda_layers/awswrangler-layer-2.15.1-py3.9.zip"),
            layer_version_name=f"{context.environment}-awswrangler-lambda-layer",
            compatible_runtimes=[aws_lambda.Runtime.PYTHON_3_9],
            compatible_architectures=[aws_lambda.Architecture.X86_64],
        )

        # Define the Lambda policy for the Lambda
        inline_lambda_policy = aws_iam.ManagedPolicy(
            self,
            "LambdaInlinePolicy",
            description="Used for Lambda permissions",
            managed_policy_name=f"{context.environment}-lambda-policy",
            statements=[
                aws_iam.PolicyStatement(
                    sid="CloudWatchLogsAccess",
                    effect=aws_iam.Effect.ALLOW,
                    actions=[
                        "logs:CreateLogGroup",
                        "logs:PutLogEvents",
                        "logs:CreateLogStream",
                    ],
                    resources=[
                        f"arn:aws:logs:{context.region}:{context.accountNumber}:log-group:/aws/lambda/*"
                    ],
                ),
                aws_iam.PolicyStatement(
                    sid="LambdaS3Access",
                    effect=aws_iam.Effect.ALLOW,
                    actions=[
                        "s3:GetObject",
                        "s3:GetObjectVersion",
                        "s3:ListBucket",
                        "s3:PutObject",
                        "s3:PutObjectAcl",
                        "s3:PutObjectTagging",
                        "s3:GetObjectTagging",
                        "s3:DeleteObject"
                    ],
                    resources=[
                        s3_bucket.bucket_arn,
                        s3_bucket.bucket_arn + "/*",
                        data_lake_bucket.bucket_arn,
                        data_lake_bucket.bucket_arn + "/*",
                    ],
                ),
                aws_iam.PolicyStatement(
                    sid="LambdaDynamoDBTableAccess",
                    effect=aws_iam.Effect.ALLOW,
                    actions=[
                        "dynamodb:GetItem",
                        "dynamodb:PutItem",
                        "dynamodb:UpdateItem"
                    ],
                    resources=[
                        global_variables_table.table_arn,
                        dynamo_table.table_arn
                    ],
                ),
                aws_iam.PolicyStatement(
                    sid="LambdaSNSAccess",
                    effect=aws_iam.Effect.ALLOW,
                    actions=[
                        "sns:Publish",
                        "sns:Subscribe",
                        "sns:ListSubscriptionsByTopic"
                    ],
                    resources=[
                        sns_success_topic.topic_arn,
                        sns_failure_topic.topic_arn
                    ],
                ),
                aws_iam.PolicyStatement(
                    sid="LambdaGlueAccess",
                    effect=aws_iam.Effect.ALLOW,
                    actions=[
                        "glue:*",
                    ],
                    resources=[
                        "*"
                    ],
                ),
                aws_iam.PolicyStatement(
                    sid="LambdaDataAPIAccess",
                    effect=aws_iam.Effect.ALLOW,
                    actions=[
                        "redshift-data:BatchExecuteStatement",
                        "redshift-data:ExecuteStatement",
                        "redshift-data:CancelStatement",
                        "redshift-data:ListStatements",
                        "redshift-data:GetStatementResult",
                        "redshift-data:DescribeStatement",
                        "redshift-data:ListDatabases",
                        "redshift-data:ListSchemas",
                        "redshift-data:ListTables",
                        "redshift-data:DescribeTable"
                    ],
                    resources=[
                        "*"
                    ],
                ),
                aws_iam.PolicyStatement(
                    sid="LambdaDataIAMAccess",
                    effect=aws_iam.Effect.ALLOW,
                    actions=[
                        "iam:ListRoles",
                        "iam:ListUsers",
                        "iam:ListGroups",
                        "iam:ListPolicies"
                    ],
                    resources=[
                        "*"
                    ],
                ),
                aws_iam.PolicyStatement(
                    sid="LambdaSecretsManagerAccess",
                    effect=aws_iam.Effect.ALLOW,
                    actions=["secretsmanager:GetSecretValue"],
                    resources=[
                        f"{context.rs_secret_arn}",
                        etl_user_secret.secret_arn
                    ],
                ),
            ],
        )

        # Define Lambda Role with inline policies attached
        lambda_role = aws_iam.Role(
            self,
            "LambdaIAMRole",
            role_name=f"{context.environment}-lambda-role",
            assumed_by=aws_iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                aws_iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"
                ),
                inline_lambda_policy,
            ],
        )

        # Lambda Construct
        lambda_function = aws_lambda.Function(
            self,
            "ConstructLambda",
            role=lambda_role,
            runtime=aws_lambda.Runtime.PYTHON_3_9,
            handler="lambda_function.lambda_handler",
            security_groups=[security_group],
            environment={
                "ENVIRONMENT_NAME": f"{context.environment}"
            },
            code=aws_lambda.Code.from_asset("apps/lambda_functions"),
            timeout=Duration.minutes(5),
            function_name=f"{context.environment}-upload-trigger-lamda",
            memory_size=512,
            layers=[awswrangler_layer],
            retry_attempts=0,
        )

        # S3 Upload Trigger For Lambda Function when file is uploaded under "inbound/" prefix
        s3_bucket.add_event_notification(s3.EventType.OBJECT_CREATED,
                                                     aws_s3_notifications.LambdaDestination(lambda_function),
                                                     s3.NotificationKeyFilter(prefix="inbound/"))
