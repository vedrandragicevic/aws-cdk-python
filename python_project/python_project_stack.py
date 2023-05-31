"""
AWS GLUE: https://docs.aws.amazon.com/cdk/api/v1/python/aws_cdk.aws_glue/README.html
LAKE FORMATION EXAMPLE: https://catalog.us-east-1.prod.workshops.aws/workshops/697be460-9224-4b82-99e2-5103b900ed4e/en-US/030-build/034-code-walkthrough
GITHUB EXAMPLES:
    https://github.com/aws-samples/aws-glue-cdk-cicd/tree/main
    https://github.com/aws-samples/aws-cdk-examples/tree/master/python
    https://github.com/aws-samples/aws-cdk-pipelines-datalake-etl
    https://github.com/aws-samples/aws-cdk-pipelines-datalake-infrastructure

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

        # ===================================================================================
        # ================================= S3 ==============================================
        # ===================================================================================

        # S3 Bucket Construct
        data_ingestion_bucket = s3.Bucket(self, "IngestionBucket",
                                          bucket_name=f"data-ingestion-{str(uuid.uuid4()).replace('-', '')}",
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
                                                table_name=f"global_variables",
                                                removal_policy=RemovalPolicy.DESTROY
                                                )

        glue_control_job_table = dynamodb.Table(self, "GlueControlJobTable",
                                                partition_key=dynamodb.Attribute(
                                                    name="name", type=dynamodb.AttributeType.STRING),
                                                billing_mode=dynamodb.BillingMode.PROVISIONED,
                                                table_name=f"glue_control_job",
                                                removal_policy=RemovalPolicy.DESTROY
                                                )

        glue_control_job_step_table = dynamodb.Table(self, "GlueControlJobStepTable",
                                                     partition_key=dynamodb.Attribute(
                                                         name="name", type=dynamodb.AttributeType.STRING),
                                                     billing_mode=dynamodb.BillingMode.PROVISIONED,
                                                     table_name=f"glue_control_job_step",
                                                     removal_policy=RemovalPolicy.DESTROY
                                                     )

        # ===================================================================================
        # =========================== SECRETS MANAGER =======================================
        # ===================================================================================

        sm_secret = aws_secretsmanager.Secret(self, "SecretSecretsManager",
                                                     secret_name=f"{context.environment}-sm-credentials",
                                                     description="Credentials test server")

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
            "IceIntegrationGluePolicy",
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
                    resources=[f"{ice_data_ingest_log_group.log_group_arn}:*",
                               f"{ice_adler_ftp_log_group.log_group_arn}:*"],
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
                        "s3:DeleteObject",
                        "s3:DeleteObjectVersion",
                        "s3:PutObjectAcl",
                        "s3:ListBucketVersions",
                    ],
                    resources=[
                        data_ingestion_bucket.bucket_arn,
                        data_ingestion_bucket.bucket_arn + "/*"
                    ],
                ),
                aws_iam.PolicyStatement(
                    sid="SecretsManagerAccess",
                    effect=aws_iam.Effect.ALLOW,
                    actions=["secretsmanager:GetSecretValue"],
                    resources=[
                        sm_secret.secret_arn
                    ],
                ),

                aws_iam.PolicyStatement(
                    sid="SecretValueRetrieveAccess",
                    effect=aws_iam.Effect.ALLOW,
                    actions=["secretsmanager:GetSecretValue"],
                    resources=["*"],
                )

            ],
        )

        # Define Glue Role For Data Ingestion
        data_ingest_glue_role = aws_iam.Role(
            self,
            "IntegrationGlueRole",
            role_name=f"glue-role",
            assumed_by=aws_iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                aws_iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSGlueServiceRole"
                ),
                data_ingest_glue_policy,
            ],
        )

        sm_secret.grant_read(data_ingest_glue_role)
        sm_secret.grant_write(data_ingest_glue_role)

        # Python Shell Glue Job Construct
        ingestion_glue_job = glue_alpha.Job(
            self,
            "IngestionGlueJob",
            job_name=f"ingest-flat-files",
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
                # Parameter --extra-py-files references .whl files from where glue job will install extra dependencies
                "--extra-py-files": f"s3://{glue_job_scripts_bucket.bucket_name}/lib/awswrangler-2.10.0-py3-none-any.whl, s3://{glue_job_scripts_bucket.bucket_name}/lib/simpledbf-0.2.6-py3-none-any.whl",
                "--TempDir": f"s3://{glue_job_scripts_bucket.bucket_name}/temporary/"
            },
        )

        ingestion_glue_job.apply_removal_policy(RemovalPolicy.DESTROY)
        ingestion_glue_job.role.add_managed_policy(
            aws_iam.ManagedPolicy.from_aws_managed_policy_name('AWSGlueConsoleFullAccess')
        )

        # ===================================================================================
        # ================================= CRAWLER =========================================
        # ===================================================================================

        # Data Lake Setup
        data_lake_bucket = s3.Bucket(self, "DataLakeBucket",
                                     bucket_name=f"data-lake-{str(uuid.uuid4()).replace('-', '')}",
                                     encryption=s3.BucketEncryption.S3_MANAGED,
                                     removal_policy=RemovalPolicy.DESTROY
                                     )

        glue_database = glue_alpha.Database(self,
                                            "GlueDatabaseLakeFormation",
                                            database_name="dev-test-table",
                                            location_uri=f"s3://{data_lake_bucket.bucket_name}/sub_folder")
        glue_database.apply_removal_policy(RemovalPolicy.DESTROY)

        #  create a glue crawler to build the data catalog
        # Step 1 . create a role for AWS Glue
        glue_crawler_role = aws_iam.Role(self, "glue_role_crawler",
                                         assumed_by=aws_iam.ServicePrincipal('glue.amazonaws.com'),
                                         managed_policies=[
                                             aws_iam.ManagedPolicy.from_managed_policy_arn(self,
                                                                                           'MyFitsCrawlerGlueRole',
                                                                                           managed_policy_arn='arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole')]
                                         )

        # glue role needs "*" read/write
        # otherwise crawler will not be able to create tables (and no error messages in crawler logs)
        glue_crawler_role.add_to_policy(
            aws_iam.PolicyStatement(actions=['s3:GetObject', 's3:PutObject', 'lakeformation:GetDataAccess'],
                                    effect=aws_iam.Effect.ALLOW, resources=['*']))

        # Glue Crawler Definition
        s3_target = glue.CfnCrawler.S3TargetProperty(path=f"{data_ingestion_bucket.bucket_name}/subfolder1/subfolder2/")
        glue_crawler = glue.CfnCrawler(self,
                                       'GlueCrawlerDefinition',
                                       description='GlueCrawlerDesc',
                                       name='dev-lake-formation-crawler',
                                       database_name=glue_database.database_name,
                                       role=glue_crawler_role.role_arn,
                                       targets=glue.CfnCrawler.TargetsProperty(s3_targets=[s3_target]),
                                       # schedule={"scheduleExpression": "cron(0/15 * * * ? *)"}, RUN EVERY 15 MIN
                                       )

        glue_crawler.apply_removal_policy(RemovalPolicy.DESTROY)

        # Tags
        Tags.of(glue_crawler).add("Key", "Value")

        # ===================================================================================
        # =========================== LAKE FORMATION ========================================
        # ===================================================================================

        """
            Lake Formation constructs defined are not working properly. Use boto3 deployment script for LF.
        """
        # The AWS::LakeFormation::Resource represents the data ( buckets and folders)
        # that is being registered with AWS Lake Formation

        lake_formation_resource = lakeformation.CfnResource(self, "LakeFormationResourceID",
                                                            resource_arn=data_lake_bucket.bucket_arn,
                                                            use_service_linked_role=False,
                                                            # the properties below are optional
                                                            # role_arn="roleArn",
                                                            # with_federation=False
                                                            )

        # The AWS::LakeFormation::Permissions resource represents the permissions that a principal has
        # on an AWS Glue Data Catalog resource (such as AWS Glue database or AWS Glue tables)
        lakeformation.CfnPermissions(self, "MyDataLakeDatabasePermission",
                                     data_lake_principal=lakeformation.CfnPermissions.DataLakePrincipalProperty(
                                         data_lake_principal_identifier=glue_crawler_role.role_arn),
                                     resource=lakeformation.CfnPermissions.ResourceProperty(
                                         database_resource=lakeformation.CfnPermissions.DatabaseResourceProperty(
                                             name=glue_database.database_name)),
                                     permissions=["ALTER", "DROP", "CREATE_TABLE"],
                                     )

        location_permission = lakeformation.CfnPermissions(self, "MyFitsDatalakeLocationPermission",
                                                           data_lake_principal=lakeformation.CfnPermissions.DataLakePrincipalProperty(
                                                               data_lake_principal_identifier=glue_crawler_role.role_arn),
                                                           resource=lakeformation.CfnPermissions.ResourceProperty(
                                                               data_location_resource=lakeformation.CfnPermissions.DataLocationResourceProperty(
                                                                   s3_resource=data_lake_bucket.bucket_arn)),
                                                           permissions=["DATA_LOCATION_ACCESS"],
                                                           )

        # make sure the location resource is created first
        location_permission.node.add_dependency(lake_formation_resource)

        # The AWS::LakeFormation::Tag resource represents an LF-tag,
        # which consists of a key and one or more possible values for the key

        lf_tags = lakeformation.CfnTag(self, "MyCfnTag",
                                      tag_key="LFtagKey",
                                      tag_values=["LFtagValues"]
                                      )
        lf_tags.apply_removal_policy(RemovalPolicy.DESTROY)

        # The AWS::LakeFormation::TagAssociation resource represents an assignment of an LF-tag
        # to a Data Catalog resource (database, table, or column)
        """cfn_tag_association = lakeformation.CfnTagAssociation(
            self, "MyCfnTagAssociation", lf_tags=[lf_tags
            ],
            resource=lakeformation.CfnTagAssociation.ResourceProperty(
                                                            database=lakeformation.CfnTagAssociation.DatabaseResourceProperty(
                                                                # catalog_id="catalogId",
                                                                name=glue_database.database_name
                                                                )
                                                              )
                                                            )"""

        # =========================== GLUE ORCHESTRATION ====================================
        # Defining Glue workflow
        glue_workflow = glue.CfnWorkflow(
            self,
            "IngestionGlueWorkflow",
            name=f"{context.environment}-ingestion-wf",
            max_concurrent_runs=1,
            description="Data Ingestion WF"
        )

        # Defining Glue start trigger
        start_trigger = glue.CfnTrigger(
            self,
            "StartWFGlueTrigger",
            name=f"{context.environment}-tgg-rentalman-start",
            actions=[
                glue.CfnTrigger.ActionProperty(
                    job_name=ingestion_glue_job.job_name,
                    arguments={
                        "--CONTROL_JOB": "test"
                    }
                )
            ],
            type="SCHEDULED",
            workflow_name=glue_workflow.name,
            schedule=f"cron(0 8 * * ? *)",  # Run at 08:00 UTC every day
            start_on_creation=True,
        )