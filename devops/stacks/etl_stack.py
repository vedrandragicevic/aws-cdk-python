from aws_cdk import (
    Tags,
    Duration,
    Stack,
    aws_dynamodb as dynamodb,
    aws_iam,
    RemovalPolicy,
    aws_logs,
    aws_glue_alpha as glue_alpha

)
from constructs import Construct
from types import SimpleNamespace
from dataclasses import dataclass


@dataclass
class ETLStack(Stack):

    context: SimpleNamespace

    config_dynamo_table: dynamodb.Table
    data_ingest_log_group: aws_logs.LogGroup
    data_ingest_glue_policy: aws_iam.ManagedPolicy
    data_ingest_glue_role: aws_iam.Role
    ingestion_glue_job: glue_alpha.Job

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

    def build_etl_stack(self, context: SimpleNamespace, **kwargs):
        """ Main method to control construction of ETL stack. """

        self.config_dynamo_table = kwargs["config_dynamo_table"]
        self.context = context

        self.create_data_ingestion_log_group()

        self.create_data_ingestion_iam_policy()

        self.create_data_ingestion_iam_role()

        self.create_data_ingestion_glue_job()

    def create_data_ingestion_log_group(self):
        """ Creates log group for Glue Job. """
        self.data_ingest_log_group = aws_logs.LogGroup(
            self,
            "DataIngestJobLogGroup",
            log_group_name="aws-glue/jobs/data-ingest-job",
            retention=aws_logs.RetentionDays.THREE_MONTHS,
            removal_policy=RemovalPolicy.DESTROY,
        )

    def create_data_ingestion_iam_policy(self):
        """ Creates IAM Policy for IAM Role. """
        self.data_ingest_glue_policy = aws_iam.ManagedPolicy(
            self,
            "IceIntegrationGluePolicy",
            description="Used for Glue permissions to ingest data into data lake",
            managed_policy_name=f"{self.context.environment}-{self.context.appName}-glue-policy",
            statements=[
                aws_iam.PolicyStatement(
                    sid="CloudWatchLogsAccess",
                    effect=aws_iam.Effect.ALLOW,
                    actions=[
                        "logs:CreateLogGroup",
                        "logs:PutLogEvents",
                        "logs:CreateLogStream",
                    ],
                    resources=[f"{self.data_ingest_log_group.log_group_arn}:*"],
                ),
                aws_iam.PolicyStatement(
                    sid="DynamoDBTableAccess",
                    effect=aws_iam.Effect.ALLOW,
                    actions=[
                        "dynamodb:GetItem",
                        "dynamodb:PutItem",
                        "dynamodb:DeleteItem",
                        "dynamodb:UpdateItem"
                    ],
                    resources=[
                        self.config_dynamo_table.table_arn,
                    ]
                )
            ]
        )

    def create_data_ingestion_iam_role(self):
        """ Creates IAM Role for AWS Glue Job with the Inline Policy Attached. """
        self.data_ingest_glue_role = aws_iam.Role(
            self,
            "IntegrationGlueRole",
            role_name=f"glue-role",
            assumed_by=aws_iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                aws_iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSGlueServiceRole"
                ),
                self.data_ingest_glue_policy,
            ],
        )

    def create_data_ingestion_glue_job(self):
        # Python Shell Glue Job Construct
        self.ingestion_glue_job = glue_alpha.Job(
            self,
            "IngestionGlueJob",
            job_name=f"ingest-flat-files-vex",
            executable=glue_alpha.JobExecutable.python_shell(
                glue_version=glue_alpha.GlueVersion.V1_0,
                python_version=glue_alpha.PythonVersion.THREE,
                script=glue_alpha.Code.from_asset(f"etl/glue_jobs/ingestion_glue_job.py"),
            ),
            description="Glue Job Used To Ingest Data Into Data Lake.",
            continuous_logging=glue_alpha.ContinuousLoggingProps(
                enabled=True, log_group=self.data_ingest_log_group
            ),
            enable_profiling_metrics=True,
            role=self.data_ingest_glue_role,
            max_retries=0,
            default_arguments={
                "--CONTROL_JOB": "update_value",
                "--CONFIG_DYNAMO_TABLE": self.config_dynamo_table.table_name
            },
        )
