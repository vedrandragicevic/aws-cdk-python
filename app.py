import aws_cdk as cdk
from python_project.python_project_stack import PythonProjectStack
from aws_cdk import StackProps, App, Environment
from typing import Tuple
from types import SimpleNamespace


app = cdk.App()
PythonProjectStack(app, "PythonProjectStack")

app.synth()
