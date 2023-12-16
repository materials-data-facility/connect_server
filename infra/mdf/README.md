# MDF Connect Serverless Infrastructure

This directory includes the Terraform needed to deploy the MDF Connect 
Serverless  infrastructure. It is actually divided into three separate
Terraform projects, one for the global resources, one for dev and prod 
environments.

## Global Resources
This is the Terraform project that creates the resources that are shared between
the dev and prod environments. This includes the ECR repositories and a role 
that allows GitHub Actions to push to ECR.

## Dev and Prod Environments
These are the Terraform projects that create the dev and prod environments. They
are defined using a directory of shared modules. The two environments are 
recorded in each directory's `variables.tf` file.

### Variables
- env: The environment name. This is used to name resources and is used to select the docker image tags picked up by the lambda functions.
- namespace: The namespace to use for the resources. This makes it possible to deploy different data facilities in the same AWS account.
- mdf_secrets_arn: The ARN of the MDF Secrets Manager secret. These secrets are consumed by the Lambda functions.
- env_vars: A map of environment variables to set for the Lambda functions. Some of them are set by Terraform as a result of creating resources. Others are simply hardcoded.
- ecr_repos: A map of ECR repositories to use for the Lambda functions. 


## What it does not include
Lambda code deployment is not included in this Terraform. Instead, the lambda
functions pull their code from the attached docker images. The docker images are
built and pushed to ECR by GitHub Actions. The GitHub Actions workflows are
defined in the `../.github/workflows` directory. Images are only built and pushed
when code in the `dev` or `prod` branches are updated. The docker images are 
tagged with the originating branch name.

Association of domain names to the API Gateway is not included in this Terraform.
You will need to fiddle around in the AWS console to get this to work out 
correctly. It mostly involves creating a domain name in Route53 and then 
convincing AWS to create a certificate for it. The certificate is then used to
create a custom domain name in API Gateway. The custom domain name is then
associated with the API Gateway stage.

## What infra is there?
- An AWS API Gateway 
- An auth Lambda
- submit_dataset Lambda
- submission_status  Lambda
- Get submissions lambda
- A DynamoDB table for storing submissions


## Making changes to the existing deployment
### Pre-requisites:
- Have the terraform CLI installed on your machine. (This has been tested with version  1.5.x)
- Make sure you have the AWS CLI installed and configured with the right credentials
- Export environment variables for the AWS credentials:
  - `export AWS_ACCESS_KEY_ID=...`
  - `export AWS_SECRET_ACCESS_KEY=...`
  - `export AWS_DEFAULT_REGION=...`
- Initialize the terraform project by running `terraform init` in the directory of the environment you want to deploy from.

Steps:
- Make your edit to the terraform code.
- Run `terraform plan` to see what changes will be made.
- Run `terraform apply` to apply the changes.

## Deploying from scratch
There are a number of interesting chicken and egg problems that arise when
deploying from scratch. The following hints should get you there.

The main issue we've encountered is that we need the docker images to 
exist in the ECR repositories before we can create the lambdas. You can 
build the `global` project first and then manually push docker images to
the created repositories. The tags for these images must match the `env`
setting in the `dev` and `prod` projects. 

In order to do this, it's easiest to rely on the docker cli to push images.
You'll need to authenticate your docker cli with ECR. You can do this by
running the following command:

```shell
aws ecr get-login-password  | docker login --username AWS --password-stdin 557062710055.dkr.ecr.us-east-1.amazonaws.com
```


