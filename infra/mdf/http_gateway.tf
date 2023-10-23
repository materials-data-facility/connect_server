
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
