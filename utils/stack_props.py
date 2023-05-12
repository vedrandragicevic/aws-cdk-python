from aws_cdk import StackProps, App, Environment
from typing import Tuple
import logging
import json
from types import SimpleNamespace

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def _get_context(app: App) -> SimpleNamespace:
    """Get the CDK Context from cdk.json file, parses it and gets only specific environment
    dictionary from list of environments
    :param: app - CDK App object
    :return: context - Dictionary with Global and specific environment params
    """
    try:
        # Get the current GIT branch from CDK Context when synthing the stack
        current_branch = app.node.try_get_context("currentBranch")
        logger.info(f"Current git branch: {current_branch}")

        # Iterate over all env's (dev, valid, prod)
        environments = app.node.try_get_context("environments")
        for env in environments:
            # Use only parameters for the current GIT branch
            if env["branchName"] == current_branch:
                environment = env
        logger.info(json.dumps(environment, indent=2))

        # Get Globals params
        global_params = app.node.try_get_context("globals")
        logger.info(f"Globals: ")
        logger.info(json.dumps(global_params, indent=2))
        total_params = {**global_params, **environment}

        return SimpleNamespace(**total_params)

    except BaseException as error:
        logger.error(repr(error))


def _define_stack_props(context: SimpleNamespace) -> dict:
    """Define the StackProps object based on received context dict
    :param: context - Dictionary containting information on Globals and specific environment from cdk.json
    :return: stack_props - CDK StackProps object with env, name and stack description
    """
    # env": Environment(region=context.region, account=context.accountNumber),
    stack_props = {
        "env": Environment(region=context.region, account=context.accountNumber),
        "stack_name": f"{context.environment}-{context.appName}-stack",
        "description": "CDK stack used to instantiate infrastructure for Ironclad.",
    }
    return stack_props


def get_stack_props(app: App) -> Tuple[SimpleNamespace, dict]:
    """Get Context and StackProps and return to be used in app.py when creating CDK Stack
    :param: app - CDK App object
    :return: context, stack_props - Tuple containing both context dict and StackProps object
    """
    # Get context based on cdk.json
    context = _get_context(app=app)
    logger.info(f"CONTEXT: {context}")
    # Get stack props
    stack_props = _define_stack_props(context=context)
    return context, stack_props
