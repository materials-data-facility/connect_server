# Create the Lambda execution role
resource "aws_iam_role" "lambda_execution_role" {
  name = "${var.namespace}-LambdaExecutionRole-${var.env}"
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


resource "aws_iam_role_policy_attachment" "AWSLambdaBasicExecutionRole" {
  role = aws_iam_role.lambda_execution_role.id
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_policy" "allow_mdf_secrets_access_policy" {
  name        = "mdf_secrets-${var.env}"
  description = "Allow access to MDF secrets"

  policy = jsonencode({
    "Version" : "2012-10-17",
    "Statement" : [
      {
        "Effect" : "Allow",
        "Action" : [
          "secretsmanager:GetSecretValue"
        ],
        "Resource" : [ var.mdf_secrets_arn ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "mdf_secrets_policy" {
  role       = aws_iam_role.lambda_execution_role.id
  policy_arn = aws_iam_policy.allow_mdf_secrets_access_policy.arn
}

resource "aws_iam_policy" "lambda_dynamodb_policy" {
  name        = "lambda_dynamodb-policy-${var.env}"
  description = "DynamoDB Policy for Lambdas"

  # Define the policy document that grants access to DynamoDB
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Action   = [
          "dynamodb:DescribeTable",
          "dynamodb:GetItem",
          "dynamodb:PutItem",
          "dynamodb:Query",
          "dynamodb:Scan",
        ],
        Effect   = "Allow",
        Resource = [ var.dynamo_db_arn ]
      },
    ],
  })
}

resource "aws_iam_role_policy_attachment" "mdf_dynamo_policy" {
  role       = aws_iam_role.lambda_execution_role.id
  policy_arn = aws_iam_policy.lambda_dynamodb_policy.arn
}
