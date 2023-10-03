# Create an integration for each Lambda function in each environment
resource "aws_apigatewayv2_integration" "auth" {
  for_each = local.environments
  api_id             = aws_apigatewayv2_api.http_api[each.key].id
  integration_type   = "AWS_PROXY"
  integration_uri    = "arn:aws:lambda:${local.region}:${local.account_id}:function:${local.namespace}-auth-${each.key}"
  #integration_uri    = "arn:aws:lambda:${local.region}:${local.account_id}:function:$${stageVariables.auth_function}"
  #integration_uri    = aws_lambda_function.mdf-connect-containerized-auth[each.key].invoke_arn
  #integration_uri    = "aws_lambda_function.$local.namespace-auth-$each.key".invoke_arn
  integration_method = "POST"
  #integration_payload_format_version = "2.0"
  #integration_timeout_ms = 5000
  #route_key          = "POST /auth"
}


resource "aws_apigatewayv2_integration" "submit_dataset" {
  for_each = local.environments
  api_id             = aws_apigatewayv2_api.http_api[each.key].id
  integration_type   = "AWS_PROXY"
  #integration_uri    = "arn:aws:lambda:${local.region}:${local.account_id}:function:${local.namespace}-submit-$${stageVariables.name}"
  integration_uri    = "arn:aws:lambda:${local.region}:${local.account_id}:function:${local.namespace}-submit-${each.key}"
  #integration_uri    = "aws_lambda_function.${local.namespace}-submit-${each.key}".arn
  #integration_uri    = "arn:aws:lambda:us-east-1:557062710055:function:MDF-Connect-submit-prod"
  #integration_uri    = "arn:aws:apigateway:${local.region}:lambda:path/2015-03-31/functions/arn:aws:lambda:${local.region}:${local.account_id}:function:$${stageVariables.submit_function}/invocations"
  #integration_uri    = "arn:aws:lambda:${local.region}:${local.account_id}:function:$${stageVariables.submit_function}"
  #integration_uri    = aws_lambda_function.mdf-connect-containerized-submit[each.key].invoke_arn
  #integration_uri    = "aws_lambda_function.${local.namespace}-submit-${each.key}".invoke_arn
  integration_method = "POST"
  #integration_payload_format_version = "2.0"
  #integration_timeout_ms = 5000
  #route_key          = "POST /submit-dataset"
}

resource "aws_apigatewayv2_integration" "submission_status" {
  for_each = local.environments
  api_id             = aws_apigatewayv2_api.http_api[each.key].id
  #api_id             = aws_apigatewayv2_api.http_api.id
  integration_type   = "AWS_PROXY"
  integration_uri    = "arn:aws:lambda:${local.region}:${local.account_id}:function:${local.namespace}-status-${each.key}"
  #integration_uri    = "arn:aws:lambda:${local.region}:${local.account_id}:function:$${stageVariables.status_function}"
  #integration_uri    = aws_lambda_function.mdf-connect-containerized-status[each.key].invoke_arn
  #integration_uri    = "aws_lambda_function.${local.namespace}-status-${each.key}".invoke_arn
  integration_method = "POST"
  #integration_payload_format_version = "2.0"
  #integration_timeout_ms = 5000
  #route_key          = "POST /submission-status"
}
