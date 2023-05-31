import aws_cdk as cdk
from python_project.python_project_stack import PythonProjectStack
from aws_cdk import StackProps, App, Environment
from typing import Tuple
from types import SimpleNamespace

from utils.stack_props import get_cdk_env


app = cdk.App()

""" 
# Another way for defining context env:

context, stack_props = get_cdk_env(app=app)

PythonProjectStack(app, "PythonProjectStack", SimpleNamespace(**context), **stack_props)

"""


PythonProjectStack(app, "PythonProjectStack")

app.synth()
