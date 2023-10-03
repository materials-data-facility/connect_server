resource "aws_apigatewayv2_authorizer" "globus-auth" {
  for_each = local.environments
  api_id                            = aws_apigatewayv2_api.http_api[each.key].id
  authorizer_type                   = "REQUEST"
  #authorizer_uri = "arn:aws:lambda:${local.region}:${local.account_id}:function:MDF-Connect-auth-prod/invocations"
  authorizer_uri                    = aws_lambda_function.mdf-connect-containerized-auth[each.key].invoke_arn
  #authorizer_uri                    = aws_lambda_function..invoke_arn
  identity_sources                  = ["$request.header.Authorization"]
  name                              = "globus-auth-authorizer-${each.key}"
  authorizer_payload_format_version = "2.0"
}

resource "aws_apigatewayv2_route" "submit" {
  for_each = local.environments
  api_id    = aws_apigatewayv2_api.http_api[each.key].id
  route_key = "POST /submit"
  authorizer_id = aws_apigatewayv2_authorizer.globus-auth[each.key].id
  #authorizer_id = aws_apigatewayv2_authorizer.globus-auth[${stageVariables.name}].id
  authorization_type = "CUSTOM"
  #target = "integrations/${aws_apigatewayv2_integration.submit_dataset[each.key].id}"
  target = "integrations/${aws_apigatewayv2_integration.submit_dataset[each.key].id}"
#aws_lambda_function.mdf-connect-containerized-auth["${each.key}"].function_name
}

resource "aws_apigatewayv2_route" "submission_status" {
  for_each = local.environments
  api_id    = aws_apigatewayv2_api.http_api[each.key].id
  route_key = "GET /status/{source_id}"
  authorizer_id = aws_apigatewayv2_authorizer.globus-auth[each.key].id
  authorization_type = "CUSTOM"

  target = "integrations/${aws_apigatewayv2_integration.submission_status[each.key].id}"
}

#resource "aws_apigatewayv2_route" "submit" {
#  for_each = local.environments
#  api_id    = aws_apigatewayv2_api.http_api[each.key].id
#  route_key = "POST /submit"
#
#  target = "integrations/${aws_apigatewayv2_integration.submit_dataset[each.key].id}"
#}
