

resource "aws_apigatewayv2_api" "mdf_connect_api" {
  name               = "mdf_connect_api-${var.env}"
  protocol_type = "HTTP"
}

resource "aws_apigatewayv2_stage" "mdf_connect_api" {
  api_id = aws_apigatewayv2_api.mdf_connect_api.id
  name        = "mdf_connect_${var.env}"
  auto_deploy = true

  default_route_settings{
    throttling_burst_limit = 5000
    throttling_rate_limit = 10000
  }

  access_log_settings {
    destination_arn = aws_cloudwatch_log_group.mdf_connect_api_gw.arn

    format = jsonencode({
      requestId               = "$context.requestId"
      sourceIp                = "$context.identity.sourceIp"
      httpMethod              = "$context.httpMethod"
      resourcePath            = "$context.resourcePath"
      routeKey                = "$context.routeKey"
      status                  = "$context.status"
      responseLength          = "$context.responseLength"
      integrationErrorMessage = "$context.integrationErrorMessage"
      errorMessage            = "$context.error.message"
      errorType               = "$context.error.responseType"
      protocol                = "$context.protocol"
      requestTime             = "$context.requestTime"
      integrationRequestId    = "$context.integration.requestId"
      functionResponseStatus  = "$context.integration.status"
      integrationLatency      = "$context.integration.latency"
      integrationServiceStatus= "$context.integration.integrationStatus"
      responseLatency         = "$context.responseLatency"
      path                    = "$context.path"
      authorizerServiceStatus = "$context.authorizer.status"
      authorizerLatency       = "$context.authorizer.latency"
      authorizerRequestId     = "$context.authorizer.requestId"
      }
    )
  }
}

resource "aws_cloudwatch_log_group" "mdf_connect_api_gw" {
  name = "/aws/mdf/${aws_apigatewayv2_api.mdf_connect_api.name}"

  retention_in_days = 30
}


# Globus Auth Authorizer to protect the API
resource "aws_apigatewayv2_authorizer" "mdf_connect_authorizer" {
  api_id                            =   aws_apigatewayv2_api.mdf_connect_api.id
  authorizer_type                   = "REQUEST"
  identity_sources                  = ["$request.header.Authorization"]
  authorizer_payload_format_version = "2.0"
  authorizer_result_ttl_in_seconds  = 0 # disable caching

  authorizer_uri                    = var.mdf_connect_authorizer_invoke_arn
  name                              = var.mdf_connect_authorizer_function_name
}

resource "aws_lambda_permission" "auth_lambda_permission" {
  statement_id  = "AllowExecutionFromAPIGateway"
  action        = "lambda:InvokeFunction"
  function_name = var.mdf_connect_authorizer_function_name
  principal     = "apigateway.amazonaws.com"

  source_arn = "${aws_apigatewayv2_api.mdf_connect_api.execution_arn}/*/*"
}

# Submit Dataset
resource "aws_apigatewayv2_integration" "submit_dataset_integration" {
  api_id = aws_apigatewayv2_api.mdf_connect_api.id

  integration_type = "AWS_PROXY"
  integration_uri  = var.submit_lambda_invoke_arn
}

resource "aws_apigatewayv2_route" "submit_dataset_route" {
  api_id    = aws_apigatewayv2_api.mdf_connect_api.id
  route_key = "POST /submit"
  authorizer_id = aws_apigatewayv2_authorizer.mdf_connect_authorizer.id
  authorization_type = "CUSTOM"

  target = "integrations/${aws_apigatewayv2_integration.submit_dataset_integration.id}"
}

resource "aws_lambda_permission" "submit_lambda_permission" {
  statement_id  = "AllowExecutionFromAPIGateway"
  action        = "lambda:InvokeFunction"
  function_name = var.submit_lambda_function_name
  principal     = "apigateway.amazonaws.com"

  source_arn = "${aws_apigatewayv2_api.mdf_connect_api.execution_arn}/*/*"
}


# Submission status
resource "aws_apigatewayv2_integration" "status_integration" {
  api_id = aws_apigatewayv2_api.mdf_connect_api.id

  integration_type = "AWS_PROXY"
  integration_uri  = var.status_lambda_invoke_arn
}

resource "aws_apigatewayv2_route" "status_route" {
  api_id    = aws_apigatewayv2_api.mdf_connect_api.id
  route_key = "GET /status/{source_id}"
  authorizer_id = aws_apigatewayv2_authorizer.mdf_connect_authorizer.id
  authorization_type = "CUSTOM"

  target = "integrations/${aws_apigatewayv2_integration.status_integration.id}"
}

resource "aws_lambda_permission" "status_lambda_permission" {
  statement_id  = "AllowExecutionFromAPIGateway"
  action        = "lambda:InvokeFunction"
  function_name = var.status_lambda_function_name
  principal     = "apigateway.amazonaws.com"

  source_arn = "${aws_apigatewayv2_api.mdf_connect_api.execution_arn}/*/*"
}


# Submission history
resource "aws_apigatewayv2_integration" "submissions_integration" {
  api_id = aws_apigatewayv2_api.mdf_connect_api.id

  integration_type = "AWS_PROXY"
  integration_uri  = var.submissions_lambda_invoke_arn
}

resource "aws_apigatewayv2_route" "submissions_route" {
  api_id    = aws_apigatewayv2_api.mdf_connect_api.id
  route_key = "POST /submissions"
  authorizer_id = aws_apigatewayv2_authorizer.mdf_connect_authorizer.id
  authorization_type = "CUSTOM"

  target = "integrations/${aws_apigatewayv2_integration.submissions_integration.id}"
}

resource "aws_lambda_permission" "submissions_lambda_permission" {
  statement_id  = "AllowExecutionFromAPIGateway"
  action        = "lambda:InvokeFunction"
  function_name = var.submissions_lambda_function_name
  principal     = "apigateway.amazonaws.com"

  source_arn = "${aws_apigatewayv2_api.mdf_connect_api.execution_arn}/*/*"
}
