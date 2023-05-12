#!/usr/bin/env python3
import os

import aws_cdk as cdk

from aws_cdk_python.aws_cdk_python_stack import DataPlatformStack

from aws_cdk import StackProps, App, Environment
from typing import Tuple
import logging
import json
from types import SimpleNamespace

logger = logging.getLogger()
logger.setLevel(logging.INFO)


app = cdk.App()

current_branch = app.node.try_get_context("currentBranch")
logger.info(f"Current git branch: {current_branch}")
print(f"Current git branch: {current_branch}")

environments = app.node.try_get_context(current_branch)

if environments["branchName"] == current_branch:
    environment = environments
logger.info(json.dumps(environment, indent=2))
print(f"Json.dumps")
print(json.dumps(environment, indent=2))

# Get Globals params
global_params = app.node.try_get_context("globals")
logger.info(f"Globals: ")
print(f"Globals: ")
print(json.dumps(global_params, indent=2))
logger.info(json.dumps(global_params, indent=2))
context = {**global_params, **environment}
print(f"Context: ")
print({**global_params, **environment})

stack_props = {
        "env": Environment(region=context['region'], account=context['accountNumber']),
        "stack_name": f"{context['environment']}-{context['appName']}-stack",
        "description": "CDK stack used to instantiate infrastructure for DATA INGESTION.",
    }

DataPlatformStack(app, "DataPlatformStack", SimpleNamespace(**context), **stack_props)

app.synth()
