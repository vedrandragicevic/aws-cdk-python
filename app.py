from aws_cdk import (
    App,
    Environment
)
from devops.stacks.docs_deployment_stack import DocsDeploymentStack

# Constant strings
ACCOUNT_ID = "012345"
REGION = "us-east-1"

app = App()

################################
# DOCS DEPLOYMENT STACK
################################
DocsDeploymentStack(app, "dev-docs-deployment-pipeline", env=Environment(account=ACCOUNT_ID, region=REGION))

app.synth()
