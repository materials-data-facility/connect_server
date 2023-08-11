resource "aws_apigatewayv2_authorizer" "globus-auth" {
  api_id                            = aws_apigatewayv2_api.http_api.id
  authorizer_type                   = "REQUEST"
  #authorizer_uri = "arn:aws:lambda:${local.region}:${local.account_id}:function:MDF-Connect-auth-prod/invocations"
  authorizer_uri                    = aws_lambda_function.mdf-connect-containerized-auth["prod"].invoke_arn
  #authorizer_uri                    = aws_lambda_function..invoke_arn
  identity_sources                  = ["$request.header.Authorization"]
  name                              = "globus-auth-authorizer"
  authorizer_payload_format_version = "2.0"
}

resource "aws_apigatewayv2_route" "submit" {
  api_id    = aws_apigatewayv2_api.http_api.id
  route_key = "POST /submit"
  authorizer_id = aws_apigatewayv2_authorizer.globus-auth.id
  authorization_type = "CUSTOM"
  target = "integrations/${aws_apigatewayv2_integration.submit_dataset.id}"
}

resource "aws_apigatewayv2_route" "submission_status" {
  api_id    = aws_apigatewayv2_api.http_api.id
  route_key = "GET /status/{source_id}"
  authorizer_id = aws_apigatewayv2_authorizer.globus-auth.id
  authorization_type = "CUSTOM"

  target = "integrations/${aws_apigatewayv2_integration.submission_status.id}"
}

#resource "aws_apigatewayv2_route" "submit" {
#  for_each = local.environments
#  api_id    = aws_apigatewayv2_api.http_api[each.key].id
#  route_key = "POST /submit"
#
#  target = "integrations/${aws_apigatewayv2_integration.submit_dataset[each.key].id}"
#}
