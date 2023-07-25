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


def get_stack_props(app: App) -> SimpleNamespace:
    """Get Context to be used in app.py when creating CDK Stack
    :param: app - CDK App object
    :return: context
    """
    # Get context based on cdk.json
    context = _get_context(app=app)
    return context
