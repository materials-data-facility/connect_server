variable "auth-function-name" {
  default = "mdf-connect-auth"
}


resource "aws_lambda_function" "mdf-connect-auth" {
  function_name = "${var.namespace}-${var.auth-function-name}-${var.env}"
  description   = "lambda function from terraform"
  image_uri     = "${var.ecr_repos["auth"]}:${var.env}"
  package_type  = "Image"
  architectures = ["x86_64"]
  role          = var.lambda_execution_role_arn
  timeout = 30
  environment {
      variables = var.env_vars
  }
}


resource "aws_lambda_function" "mdf-connect-submit" {
  function_name = "${var.namespace}-submit-${var.env}"
  description   = "lambda function from terraform"
  image_uri     = "${var.ecr_repos["submit"]}:${var.env}"
  package_type  = "Image"
  architectures = ["x86_64"]
  role          = var.lambda_execution_role_arn
  timeout = 30
  environment {
      variables = var.env_vars
  }
}

resource "aws_lambda_function" "mdf-connect-status" {
  function_name = "${var.namespace}-status-${var.env}"
  description   = "lambda function from terraform"
  image_uri     = "${var.ecr_repos["status"]}:${var.env}"
  package_type  = "Image"
  architectures = ["x86_64"]
  role          = var.lambda_execution_role_arn
  timeout = 30
  environment {
      variables = var.env_vars
  }
}


resource "aws_lambda_function" "mdf-connect-submissions" {
  function_name = "${var.namespace}-submissions-${var.env}"
  description   = "lambda function from terraform"

  image_uri     = "${var.ecr_repos["submissions"]}:${var.env}"
  package_type  = "Image"
  architectures = ["x86_64"]

  role          = var.lambda_execution_role_arn
  timeout = 30
  environment {
      variables = var.env_vars
  }
}

