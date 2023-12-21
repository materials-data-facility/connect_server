
#### Create lambda functions for MDF Connect
#### These functions are deployed as container images
#### The container images are built and pushed to ECR by the GitHub Action
#### We explicitly create the log groups so we can tag them and set the retention period
locals {
  auth_function_name = "${var.namespace}-auth-${var.env}"
  submit_function_name = "${var.namespace}-submit-${var.env}"
  status_function_name = "${var.namespace}-status-${var.env}"
  submissions_function_name = "${var.namespace}-submissions-${var.env}"
}

resource "aws_lambda_function" "mdf-connect-auth" {
  function_name = local.auth_function_name
  description   = "GlobusAuth Authorizer for MDF Connect"
  image_uri     = "${var.ecr_repos["auth"]}:${var.env}"
  package_type  = "Image"
  architectures = ["x86_64"]
  role          = var.lambda_execution_role_arn
  timeout = 30
  environment {
      variables = var.env_vars
  }
  depends_on = [aws_cloudwatch_log_group.auth_log_group]
  tags = var.resource_tags
}


resource "aws_cloudwatch_log_group" "auth_log_group" {
  name              = "/aws/lambda/${local.auth_function_name}"
  retention_in_days = 5
  tags = var.resource_tags
}

resource "aws_lambda_function" "mdf-connect-submit" {
  function_name = local.submit_function_name
  description   = "Submit Datasets via MDF Connect"
  image_uri     = "${var.ecr_repos["submit"]}:${var.env}"
  package_type  = "Image"
  architectures = ["x86_64"]
  role          = var.lambda_execution_role_arn
  timeout = 30
  environment {
      variables = var.env_vars
  }
  depends_on = [aws_cloudwatch_log_group.submit_log_group]
  tags = var.resource_tags
}

resource "aws_cloudwatch_log_group" "submit_log_group" {
  name              = "/aws/lambda/${local.submit_function_name}"
  retention_in_days = 5
  tags = var.resource_tags
}

resource "aws_lambda_function" "mdf-connect-status" {
  function_name = local.status_function_name
  description   = "Retrieve submit status via MDF Connect"
  image_uri     = "${var.ecr_repos["status"]}:${var.env}"
  package_type  = "Image"
  architectures = ["x86_64"]
  role          = var.lambda_execution_role_arn
  timeout = 30
  environment {
      variables = var.env_vars
  }
  depends_on = [aws_cloudwatch_log_group.status_log_group]
  tags = var.resource_tags
}

resource "aws_cloudwatch_log_group" "status_log_group" {
  name              = "/aws/lambda/${local.status_function_name}"
  retention_in_days = 5
  tags = var.resource_tags
}

resource "aws_lambda_function" "mdf-connect-submissions" {
  function_name = local.submissions_function_name
  description   = "Retrieve history of submissions via MDF Connect"

  image_uri     = "${var.ecr_repos["submissions"]}:${var.env}"
  package_type  = "Image"
  architectures = ["x86_64"]

  role          = var.lambda_execution_role_arn
  timeout = 30
  environment {
      variables = var.env_vars
  }
  depends_on = [aws_cloudwatch_log_group.submissions_log_group]
  tags = var.resource_tags
}

resource "aws_cloudwatch_log_group" "submissions_log_group" {
  name              = "/aws/lambda/${local.submissions_function_name}"
  retention_in_days = 5
  tags = var.resource_tags
}
