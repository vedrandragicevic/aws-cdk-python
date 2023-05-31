# How to do the boto3 deployment
- Enable the local virtual env
- Install boto3 library in your local venv
- Perform AWS CLI log in using your AWS profile (aws configure sso) and point the CLI to environment for deployment
- Make sure to cd into the 'boto3_deployment' directory
- Execute 'python lake_formation_service.py dev' for dev environment
- The script will create LF tag defined in .json config file for a current environment, register data lake locations for buckets and add tags to DBs