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
  description   = "lambda function from terraform"
  image_uri     = "${aws_ecr_repository.mdf-connect-lambda-repo["status"].repository_url}:${each.key}"
  package_type  = "Image"
  architectures = ["x86_64"]
  role          = aws_iam_role.lambda_execution.arn
  timeout = 30
  environment {
      variables = local.env_vars[each.key]
  }
}

resource "aws_lambda_function" "mdf-connect-containerized-auth" {
  for_each = local.environments
  function_name = "${local.namespace}-auth-${each.key}"
  description   = "lambda function from terraform"
  image_uri     = "${aws_ecr_repository.mdf-connect-lambda-repo["auth"].repository_url}:${each.key}"
  package_type  = "Image"
  architectures = ["x86_64"]
  role          = aws_iam_role.lambda_execution.arn
  timeout = 30
}

resource "aws_lambda_function" "mdf-connect-containerized-submit" {
  for_each = local.environments
  function_name = "${local.namespace}-submit-${each.key}"
  description   = "lambda function from terraform"
  image_uri     = "${aws_ecr_repository.mdf-connect-lambda-repo["submit"].repository_url}:${each.key}"
  package_type  = "Image"
  architectures = ["x86_64"]
  role          = aws_iam_role.lambda_execution.arn
  timeout = 30
  environment {
      variables = local.env_vars[each.key]
        }
}

data "archive_file" "python_lambda_package" {
  type = "zip"
  source_file = "${path.module}/code/submissions.py"
  output_path = "nametest.zip"
}

resource "aws_lambda_function" "mdf-connect-containerized-submissions" {
  for_each = local.environments
  function_name = "${local.namespace}-submissions-${each.key}"
  description   = "lambda function from terraform"

  filename      = "nametest.zip"
  source_code_hash = data.archive_file.python_lambda_package.output_base64sha256
  runtime       = "python3.11"
  handler       = "submissions.lambda_handler"

  role          = aws_iam_role.lambda_execution.arn
  timeout = 30
  environment {
      variables = local.env_vars[each.key]
        }
}