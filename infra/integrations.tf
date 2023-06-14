
# Create an integration for each Lambda function in each environment
resource "aws_apigatewayv2_integration" "auth_testing" {
  api_id             = aws_apigatewayv2_api.http_api.id
  integration_type   = "AWS_PROXY"
  integration_uri    = aws_lambda_function.auth["test"].invoke_arn
  integration_method = "POST"
  #integration_payload_format_version = "2.0"
  #integration_timeout_ms = 5000
  #route_key          = "POST /auth"
}

resource "aws_apigatewayv2_integration" "auth_production" {
  api_id             = aws_apigatewayv2_api.http_api.id
  integration_type   = "AWS_PROXY"
  integration_uri    = aws_lambda_function.auth["prod"].invoke_arn
  integration_method = "POST"
  #integration_payload_format_version = "2.0"
  #integration_timeout_ms = 5000
  #route_key          = "POST /auth"
}

resource "aws_apigatewayv2_integration" "submit_dataset_testing" {
  api_id             = aws_apigatewayv2_api.http_api.id
  integration_type   = "AWS_PROXY"
  integration_uri    = aws_lambda_function.submit_dataset["test"].invoke_arn
  integration_method = "POST"
  #integration_payload_format_version = "2.0"
  #integration_timeout_ms = 5000
  #route_key          = "POST /submit-dataset"
}

resource "aws_apigatewayv2_integration" "submit_dataset_production" {
  api_id             = aws_apigatewayv2_api.http_api.id
  integration_type   = "AWS_PROXY"
  integration_uri    = aws_lambda_function.submit_dataset["prod"].invoke_arn
  integration_method = "POST"
  #integration_payload_format_version = "2.0"
  #integration_timeout_ms = 5000
  #route_key          = "POST /submit-dataset"
}

resource "aws_apigatewayv2_integration" "submission_status_testing" {
  api_id             = aws_apigatewayv2_api.http_api.id
  integration_type   = "AWS_PROXY"
  integration_uri    = aws_lambda_function.submission_status["test"].invoke_arn
  integration_method = "POST"
  #integration_payload_format_version = "2.0"
  #integration_timeout_ms = 5000
  #route_key          = "POST /submission-status"
}

resource "aws_apigatewayv2_integration" "submission_status_production" {
  api_id             = aws_apigatewayv2_api.http_api.id
  integration_type   = "AWS_PROXY"
  integration_uri    = aws_lambda_function.submission_status["prod"].invoke_arn
  integration_method = "POST"
  #integration_payload_format_version = "2.0"
  #integration_timeout_ms = 5000
  #route_key          = "POST /submission-status"
}
