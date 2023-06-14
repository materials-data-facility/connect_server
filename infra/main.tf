
# Create a namespace
locals {
  namespace = "MDF-Connect"
  envs = ["test", "prod"]
  environments = toset(local.envs)
}

terraform {
  backend "s3" {
    # Replace this with your bucket name!
    bucket         = "accelerate-terraform-state-storage"
    key            = "terraform/MDF-Connect/terraform.tfstate"
    region         = "us-east-1"

    # Replace this with your DynamoDB table name!
    dynamodb_table = "accelerate-terraform-state-storage-locks"
    encrypt        = true
  }
}

# Create the Lambda execution role
resource "aws_iam_role" "lambda_execution" {
  name = "${local.namespace}-LambdaExecutionRole"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      }
    ]
  })
}

# Attach the necessary permissions policies to the role
resource "aws_iam_role_policy_attachment" "lambda_execution_permissions" {
  for_each = local.environments
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
  role       = aws_iam_role.lambda_execution.id
}

# Create the Lambda layer version using the S3 bucket
resource "aws_lambda_layer_version" "globus_layer" {
  for_each = local.environments
  layer_name          = "${local.namespace}-GlobusLayer"
  compatible_runtimes = ["python3.8"]
  filename = "globus_layer.zip"
}

# Create the Lambda functions using the S3 bucket and the Lambda layer
resource "aws_lambda_function" "auth" {
  for_each = local.environments
  function_name = "${local.namespace}-Auth-${each.key}"
  runtime = "python3.8"
  handler = "lambda_function.lambda_handler"
  role = aws_iam_role.lambda_execution.arn
  layers = [aws_lambda_layer_version.globus_layer[each.key].
arn]
  filename = "auth.zip"

  environment {
    variables = {
      ENVIRONMENT = each.key
    }
  }
}

resource "aws_lambda_function" "submit_dataset" {
  for_each = local.environments
  function_name = "${local.namespace}-SubmitDataset-${each.key}"
  runtime = "python3.8"
  handler = "lambda_function.lambda_handler"
  role = aws_iam_role.lambda_execution.arn
  layers = [aws_lambda_layer_version.globus_layer[each.key].arn]
  filename = "submit_dataset.zip"

  environment {
    variables = {
      ENVIRONMENT = each.key
    }
  }
}

resource "aws_lambda_function" "submission_status" {
  for_each = local.environments
  function_name = "${local.namespace}-SubmissionStatus-${each.key}"
  runtime = "python3.8"
  handler = "lambda_function.lambda_handler"
  role = aws_iam_role.lambda_execution.arn
  layers = [aws_lambda_layer_version.globus_layer[each.key].arn]
  filename = "submission_status.zip"

  environment {
    variables = {
      ENVIRONMENT = each.key
    }
  }
}
