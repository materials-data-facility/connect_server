data "aws_ecr_authorization_token" "token" {}

resource "aws_ecr_repository" "mdf-connect-lambda-repo" {
  for_each = local.functions
  name                 = "mdf-lambdas/${each.key}"
  image_tag_mutability = "MUTABLE"
  image_scanning_configuration {
     scan_on_push = true
  }
}

resource "aws_lambda_function" "mdf-connect-containerized-status" {
  for_each = local.environments
  function_name = "${local.namespace}-status-${each.key}"
  #handler = "lambda_function.lambda_handler"
  description   = "lambda function from terraform"
  image_uri     = "${aws_ecr_repository.mdf-connect-lambda-repo["status"].repository_url}:${each.key}"
  package_type  = "Image"
  architectures = ["x86_64"]
  role          = aws_iam_role.lambda_execution.arn
  timeout = 30
}

resource "aws_lambda_function" "mdf-connect-containerized-auth" {
  for_each = local.environments
  function_name = "${local.namespace}-auth-${each.key}"
  #handler = "lambda_function.lambda_handler"
  description   = "lambda function from terraform"
  #image_uri     = "${aws_ecr_repository.mdf-connect-lambda-repo.repository_url}/auth:latest"
  #image_uri     = "${aws_ecr_repository.mdf-connect-lambda-repo.repository_url}/auth-${local.namespace}:latest"
  image_uri     = "${aws_ecr_repository.mdf-connect-lambda-repo["auth"].repository_url}:${each.key}"
  package_type  = "Image"
  architectures = ["x86_64"]
  role          = aws_iam_role.lambda_execution.arn
  timeout = 30
}

resource "aws_lambda_function" "mdf-connect-containerized-submit" {
  for_each = local.environments
  function_name = "${local.namespace}-submit-${each.key}"
  #handler = "lambda_function.lambda_handler"
  description   = "lambda function from terraform"
  #image_uri     = "${aws_ecr_repository.mdf-connect-lambda-repo.repository_url}/submit:latest"
  image_uri     = "${aws_ecr_repository.mdf-connect-lambda-repo["submit"].repository_url}:${each.key}"
  package_type  = "Image"
  architectures = ["x86_64"]
  role          = aws_iam_role.lambda_execution.arn
  timeout = 30
}
