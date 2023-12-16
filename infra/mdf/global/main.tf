data "aws_ecr_authorization_token" "token" {}
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
    key    = "terraform/MDF-Connect/global/terraform.tfstate"
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
