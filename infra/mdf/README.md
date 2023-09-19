# MDF Connect Serverless Infrastructure

This directory includes the Terraform needed to deploy the MDF Connect Serverless  infrastructure. 

## What it does not include

Code deployment.

Deploying changes to the Lambda code is out of Terraform's purview. 
If you're making a new API endpoint that has the same auth requirements as an existing one, you probably shouldn't have need to change anything here.

## What infra is there?

- An AWS API Gateway  (2 stages, "test" and "prod")
- An auth Lambda
- submit_dataset Lambda
- submission_status  Lambda

## Making changes to the existing deployment

Prereqs:
- Have the terraform CLI installed on your machine. (I have used version > 1.3.x)
- Copy the tfvars.example to something like prod.tfvars and get the real values from the AWS console
- Use your preferred method to let Terraform authenticate with AWS

Steps:
- Make your edit to the terraform code.
- `terraform plan -var-file="{env}.tfvars"` and then if you approve, `terraform plan -var-file="{env}.tfvars"`
- Do a sanity check to make sure things are still working, and you're done :)

## Deploying from scratch

We don't want to use Terraform for our routine code deploys, but also Terraform reasonably refuses to create lambdas without code ot run on them.
The following zipfiles need to have something in them for the terraform to run:
auth.zip		submission_status.zip
globus_layer.zip	submit_dataset.zip

It needn't be the real code, but they must be valid zip files.

You can follow the shell commands in the GH Action yaml to see how to zip it up properly, but also you could just use dummy files and it would be fine.
