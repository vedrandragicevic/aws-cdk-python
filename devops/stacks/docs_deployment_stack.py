from aws_cdk import (
    aws_codecommit as codecommit,
    aws_s3 as s3,
    aws_iam as iam,
    Stack,
    App,
    Environment,
    aws_codepipeline as codepipeline,
    aws_codepipeline_actions as codepipeline_actions,
    aws_codebuild as codebuild,
    aws_events as events,
    aws_events_targets as targets
)
from constructs import Construct


class DocsDeploymentStack(Stack):
    def __init__(self, scope: Construct, id: str, **kwargs):
        super().__init__(scope, id, **kwargs)

        # CodeCommit repository
        llm_eval_repo = codecommit.Repository.from_repository_arn(
            self, "LlmEvalRepo", "arn:aws:codecommit:us-east-1:1234:repo"
        )

        # S3 bucket construct
        docs_bucket = s3.Bucket.from_bucket_name(self, "docs_bucket", bucket_name="dev-docs")

        # Create a custom IAM role for the build project
        build_role = iam.Role(self, "BuildRole",
                              assumed_by=iam.ServicePrincipal("codebuild.amazonaws.com"),
                              role_name="dev-docs-codebuild-role"
                              )

        # Add necessary policies to the build role
        build_role.add_managed_policy(iam.ManagedPolicy.from_aws_managed_policy_name("AmazonS3FullAccess"))

        # Create the pipeline role
        pipeline_role = iam.Role(self, "dev-llm-eval-docs-pipeline-IAM-Role-ID",
                                 assumed_by=iam.ServicePrincipal("codepipeline.amazonaws.com"),
                                 role_name="dev-docs-pipeline-IAM"
                                 )

        # Add necessary policies to the pipeline role
        pipeline_role.add_managed_policy(iam.ManagedPolicy.from_aws_managed_policy_name("AmazonS3FullAccess"))

        # Create the pipeline
        docs_deployment_pipeline = codepipeline.Pipeline(self, "Pipeline",
                                                         pipeline_name="dev-docs-pipeline",
                                                         role=pipeline_role
                                                         )

        # Pipeline trigger
        event_rule = events.Rule(
            self, "DocsDeploymentPipelineTrigger",
            description="Docs Deployment Trigger on CodeCommit events",
            event_pattern={
                "source": ["aws.codecommit"],
                "detail": {
                    "event": ["ReferenceCreated", "ReferenceUpdated"],
                    "referenceName": ["refs/heads/dev"],
                    "fullPath": ["docs/"]
                },
                "resources": [llm_eval_repo.repository_arn]
            }
        )

        # Event rule target
        event_rule.add_target(targets.CodePipeline(docs_deployment_pipeline))

        # Add source stage
        source_output = codepipeline.Artifact()
        docs_deployment_pipeline.add_stage(
            stage_name="Source",
            actions=[
                codepipeline_actions.CodeCommitSourceAction(
                    repository=llm_eval_repo,
                    branch="dev",
                    action_name="Source",
                    output=source_output,
                )
            ]
        )

        # Add build stage
        build_output = codepipeline.Artifact()
        docs_deployment_pipeline.add_stage(
            stage_name="Build",
            actions=[
                codepipeline_actions.CodeBuildAction(
                    action_name='Build',
                    project=codebuild.PipelineProject(self, "dev-docs", project_name="dev-project",
                                                      role=build_role,
                                                      build_spec=codebuild.BuildSpec.from_asset("buildspec.yml")),
                    input=source_output,
                    outputs=[
                        build_output
                    ],
                    environment_variables={
                        'BUCKET': codebuild.BuildEnvironmentVariable(value=docs_bucket.bucket_name)
                    }
                )
            ]
        )
