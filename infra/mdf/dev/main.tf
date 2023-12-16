data "aws_caller_identity" "current" {}

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 4.0.0"
    }
  }
  required_version = "~> 1.5.5"

  backend "s3" {
    # Replace this with your bucket name!
    bucket = "accelerate-terraform-state-storage"
    key    = "terraform/MDF-Connect/dev/terraform.tfstate"
    region = "us-east-1"

    # Replace this with your DynamoDB table name!
    dynamodb_table = "accelerate-terraform-state-storage-locks"
    encrypt        = true
  }
}

provider "aws" {
  region = "us-east-1"
  assume_role {
    role_arn = "arn:aws:iam::557062710055:role/MDFConnectAdminRole"
  }
}

module "lambdas" {
  source = "../modules/lambdas"

  env                       = var.env
  env_vars                  = module.dynamodb.updated_envs
  namespace                 = var.namespace
  lambda_execution_role_arn = module.permissions.submit_lambda_invoke_arn
  ecr_repos                 = var.ecr_repos
}

module "dynamodb" {
  source    = "../modules/dynamo"
  env       = var.env
  namespace = var.namespace
  env_vars  = var.env_vars
}

module "permissions" {
  source          = "../modules/permissions"
  env             = var.env
  namespace       = var.namespace
  mdf_secrets_arn = var.mdf_secrets_arn
  dynamo_db_arn   = module.dynamodb.dynamodb_arn
}

module "api_gateway" {
  source = "../modules/api_gateway"

  env                                  = var.env

  mdf_connect_authorizer_invoke_arn    = module.lambdas.mdf_connect_authorizer_invoke_arn
  mdf_connect_authorizer_function_name = module.lambdas.mdf_connect_authorizer_function_name

  submit_lambda_invoke_arn             = module.lambdas.submit_lambda_invoke_arn
  submit_lambda_function_name          = module.lambdas.submit_lambda_function_name

  status_lambda_invoke_arn         = module.lambdas.status_lambda_invoke_arn
  status_lambda_function_name      = module.lambdas.status_lambda_function_name

  submissions_lambda_invoke_arn    = module.lambdas.submissions_lambda_invoke_arn
  submissions_lambda_function_name = module.lambdas.submissions_lambda_function_name
}
