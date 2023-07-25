
# Sample AWS CDK project!

Project contains AWS CDK app with multiple stacks.

The `cdk.json` file serves as a context for your app.


## CDK commands

 * `cdk ls -c currentBranch=dev `          list all stacks in the app
 * `cdk synth -all -c currentBranch=dev `       emits the synthesized CloudFormation template for all stacks
 * `cdk deploy -c currentBranch=dev --all --require-approval never`      deploy all stacks to AWS account/region defined in cdk.json
 * `cdk destroy --all -c currentBranch=dev `        removes CDK stacks and resources
