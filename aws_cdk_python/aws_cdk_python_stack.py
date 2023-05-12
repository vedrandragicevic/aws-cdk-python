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
    CfnOutput,
    Aws,
)
from constructs import Construct
from types import SimpleNamespace
import uuid


class DataPlatformStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, context: SimpleNamespace, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Importing VPC
        main_vpc = ec2.Vpc.from_lookup(self, "Vpc", vpc_id=context.vpc_id)

        # ===================================================================================
        # ================================= S3 ==============================================
        # ===================================================================================

        # S3 Bucket Construct
        data_ingestion_bucket = s3.Bucket(self, "IngestionBucket",
                                              bucket_name=f"{context.environment}-ingestion-{str(uuid.uuid4()).replace('-', '')}",
                                              encryption=s3.BucketEncryption.S3_MANAGED,
                                              removal_policy=RemovalPolicy.DESTROY
                                              )

        # ===================================================================================
        # ================================= DYNAMODB ========================================
        # ===================================================================================

        # DynamoDB Table Construct
        global_variables_table = dynamodb.Table(self, "GlobalVariablesTable",
                                                partition_key=dynamodb.Attribute(
                                                    name="name", type=dynamodb.AttributeType.STRING),
                                                billing_mode=dynamodb.BillingMode.PROVISIONED,
                                                table_name=f"{context.environment}-global_variables",
                                                removal_policy=RemovalPolicy.DESTROY
                                                )

        glue_control_job_table = dynamodb.Table(self, "GlueControlJobTable",
                                                partition_key=dynamodb.Attribute(
                                                    name="name", type=dynamodb.AttributeType.STRING),
                                                billing_mode=dynamodb.BillingMode.PROVISIONED,
                                                table_name=f"{context.environment}-glue_control_job",
                                                removal_policy=RemovalPolicy.DESTROY
                                                )

        glue_control_job_step_table = dynamodb.Table(self, "GlueControlJobStepTable",
                                                     partition_key=dynamodb.Attribute(
                                                        name="name", type=dynamodb.AttributeType.STRING),
                                                     billing_mode=dynamodb.BillingMode.PROVISIONED,
                                                     table_name=f"{context.environment}-glue_control_job_step",
                                                     removal_policy=RemovalPolicy.DESTROY
                                                     )

        # ===================================================================================
        # ================================= GLUE ============================================
        # ===================================================================================

        # Defining Log Group For Glue Job
        data_ingest_log_group = aws_logs.LogGroup(
            self,
            "DataIngestJobLogGroup",
            log_group_name="aws-glue/jobs/data-ingest-job",
            retention=aws_logs.RetentionDays.THREE_MONTHS,
            removal_policy=RemovalPolicy.DESTROY,
        )

        # Define The Glue Policy For Data Ingestion
        data_ingest_glue_policy = aws_iam.ManagedPolicy(
            self,
            "IntegrationGluePolicy",
            description="Used for Glue permissions to ingest data into data lake",
            managed_policy_name=f"{context.environment}-{context.appName}-glue-policy",
            statements=[
                aws_iam.PolicyStatement(
                    sid="CloudWatchLogsAccess",
                    effect=aws_iam.Effect.ALLOW,
                    actions=[
                        "logs:CreateLogGroup",
                        "logs:PutLogEvents",
                        "logs:CreateLogStream",
                    ],
                    resources=[f"{data_ingest_log_group.log_group_arn}:*"],
                ),
                aws_iam.PolicyStatement(
                    sid="DynamoDBTableAccess",
                    effect=aws_iam.Effect.ALLOW,
                    actions=[
                        "dynamodb:GetItem",
                        "dynamodb:PutItem",
                        "dynamodb:DeleteItem",
                    ],
                    resources=[
                        global_variables_table.table_arn,
                        glue_control_job_table.table_arn,
                        glue_control_job_step_table.table_arn
                    ],
                ),
                aws_iam.PolicyStatement(
                    sid="S3AccessForGlueJob",
                    effect=aws_iam.Effect.ALLOW,
                    actions=[
                        "s3:GetObject",
                        "s3:GetObjectVersion",
                        "s3:ListBucket",
                        "s3:PutObject",
                        "s3:PutObjectAcl",
                        "s3:ListBucketVersions",
                    ],
                    resources=[
                        data_ingestion_bucket.bucket_arn,
                        data_ingestion_bucket.bucket_arn + "/*"
                    ],
                ),

            ],
        )

        # Define Glue Role For Data Ingestion
        data_ingest_glue_role = aws_iam.Role(
            self,
            "IntegrationGlueRole",
            role_name=f"{context.environment}-{context.appName}-glue-role",
            assumed_by=aws_iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                aws_iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSGlueServiceRole"
                ),
                data_ingest_glue_policy,
            ],
        )

        # Glue Job Construct
        ingestion_glue_job = glue_alpha.Job(
            self,
            "IngestionGlueJob",
            job_name=f"{context.environment}-ingest-flat-files",
            executable=glue_alpha.JobExecutable.python_shell(
                glue_version=glue_alpha.GlueVersion.V1_0,
                python_version=glue_alpha.PythonVersion.THREE,
                script=glue_alpha.Code.from_asset(f"glue_jobs/ingestion_glue_job.py"),
            ),
            description="Glue Job Used To Ingest Data Into Data Lake.",
            continuous_logging=glue_alpha.ContinuousLoggingProps(
                enabled=True, log_group=data_ingest_log_group
            ),
            enable_profiling_metrics=True,
            role=data_ingest_glue_role,
            max_retries=0,
            default_arguments={
                "--CONTROL_JOB": "update_value",
                "--GLOBAL_VARIABLES_TABLE": global_variables_table.table_name,
                "--CONTROL_JOB_TABLE": glue_control_job_table.table_name,
                "--CONTROL_JOB_STEP_TABLE": glue_control_job_step_table.table_name,
                "--additional-python-modules": "simpledbf",
            },
        )

        ingestion_glue_job.apply_removal_policy(RemovalPolicy.DESTROY)
        ingestion_glue_job.role.add_managed_policy(
            aws_iam.ManagedPolicy.from_aws_managed_policy_name('AWSGlueConsoleFullAccess')
        )
