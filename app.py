import aws_cdk as cdk
from aws_cdk import (
    Tags
)

from devops.stacks.dynamo_stack import DynamoStack
from devops.stacks.etl_stack import ETLStack
from devops_utils.stack_props import *

app = cdk.App()

context = get_stack_props(app=app)
env = context.environment.capitalize()
stack_env_object = Environment(region=context.region, account=context.accountNumber)

################################
# DYNAMO STACK
################################

dynamo_stack = DynamoStack(app, f"VexAWSDynamoStack{env}", env=stack_env_object)

# Generate CF template
dynamo_config_table = dynamo_stack.build_applications_stack(context=context)

################################
# ETL STACK
################################

etl_stack = ETLStack(app, f"VexAWSETLStack{env}", env=stack_env_object)

# Generate CF template
etl_stack.build_etl_stack(context=context,
                          config_dynamo_table=dynamo_config_table)

app.synth()
