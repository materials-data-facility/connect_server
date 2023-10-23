# Create an integration for each Lambda function in each environment
resource "aws_apigatewayv2_integration" "auth" {
  for_each = local.environments
  api_id             = aws_apigatewayv2_api.http_api[each.key].id
  integration_type   = "AWS_PROXY"
  integration_uri    = "arn:aws:lambda:${local.region}:${local.account_id}:function:${local.namespace}-auth-${each.key}"
  integration_method = "POST"
}


resource "aws_apigatewayv2_integration" "submit_dataset" {
  for_each = local.environments
  api_id             = aws_apigatewayv2_api.http_api[each.key].id
  integration_type   = "AWS_PROXY"
  integration_uri    = "arn:aws:lambda:${local.region}:${local.account_id}:function:${local.namespace}-submit-${each.key}"
  integration_method = "POST"
}

resource "aws_apigatewayv2_integration" "submission_status" {
  for_each = local.environments
  api_id             = aws_apigatewayv2_api.http_api[each.key].id
  #api_id             = aws_apigatewayv2_api.http_api.id
  integration_type   = "AWS_PROXY"
  integration_uri    = "arn:aws:lambda:${local.region}:${local.account_id}:function:${local.namespace}-status-${each.key}"
  integration_method = "GET"
}
