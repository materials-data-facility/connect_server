
# Define an API Gateway v2 HTTP API
resource "aws_apigatewayv2_api" "http_api" {
  for_each = local.environments
  name          = "MDF-Connect-http-api-${each.key}"
  protocol_type = "HTTP"
}


resource "aws_cloudwatch_log_group" "main_api_gw" {
  for_each = local.environments
  name = "/aws/api-gw/${aws_apigatewayv2_api.http_api[each.key].name}"

  retention_in_days = 14
}
# Define routes for the API Gateway v2 HTTP API
#resource "aws_apigatewayv2_route" "auth_lambda_route" {
#  api_id    = aws_apigatewayv2_api.http_api.id
#  route_key = "GET /auth"
#  #target = "lambda:${aws_lambda_function.auth["test"].function_name}"
#  target = "integrations/${aws_apigatewayv2_integration.auth_testing.id}"
#}

#resource "aws_apigatewayv2_route" "submit_dataset_lambda_route" {
#  api_id    = aws_apigatewayv2_api.http_api.id
#  route_key = "POST /submit-dataset"
#
#  #target = "lambda:${aws_lambda_function.submit_dataset["test"].function_name}"
#  target = "integrations/${aws_apigatewayv2_integration.submit_dataset_testing.id}"
#}
