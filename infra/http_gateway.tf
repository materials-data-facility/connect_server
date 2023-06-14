
# Define an API Gateway v2 HTTP API
resource "aws_apigatewayv2_api" "http_api" {
  name          = "MDF-Connect-http-api"
  protocol_type = "HTTP"
}

# Define routes for the API Gateway v2 HTTP API
resource "aws_apigatewayv2_route" "auth_lambda_route" {
  api_id    = aws_apigatewayv2_api.http_api.id
  route_key = "GET /auth"
  #target = "lambda:${aws_lambda_function.auth["test"].function_name}"
  target = "integrations/${aws_apigatewayv2_integration.auth_testing.id}"
}

resource "aws_apigatewayv2_route" "submit_dataset_lambda_route" {
  api_id    = aws_apigatewayv2_api.http_api.id
  route_key = "POST /submit-dataset"

  #target = "lambda:${aws_lambda_function.submit_dataset["test"].function_name}"
  target = "integrations/${aws_apigatewayv2_integration.submit_dataset_testing.id}"
}
