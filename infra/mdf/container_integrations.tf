# Create an integration for each Lambda function in each environment
resource "aws_apigatewayv2_integration" "auth" {
  api_id             = aws_apigatewayv2_api.http_api.id
  integration_type   = "AWS_PROXY"
  integration_uri    = "arn:aws:lambda:${local.region}:${local.account_id}:function:$${stageVariables.auth_function}"
  #integration_uri    = aws_lambda_function.mdf-connect-containerized-auth[each.key].invoke_arn
  #integration_uri    = "aws_lambda_function.$local.namespace-auth-$each.key".invoke_arn
  integration_method = "POST"
  #integration_payload_format_version = "2.0"
  #integration_timeout_ms = 5000
  #route_key          = "POST /auth"
}


resource "aws_apigatewayv2_integration" "submit_dataset" {
  api_id             = aws_apigatewayv2_api.http_api.id
  integration_type   = "AWS_PROXY"
  integration_uri    = "arn:aws:lambda:us-east-1:557062710055:function:MDF-Connect-submit-prod"
  #integration_uri    = "arn:aws:lambda:${local.region}:${local.account_id}:function:$${stageVariables.submit_function}"
  #integration_uri    = aws_lambda_function.mdf-connect-containerized-submit[each.key].invoke_arn
  #integration_uri    = "aws_lambda_function.${local.namespace}-submit-${each.key}".invoke_arn
  integration_method = "POST"
  #integration_payload_format_version = "2.0"
  #integration_timeout_ms = 5000
  #route_key          = "POST /submit-dataset"
}

resource "aws_apigatewayv2_integration" "submission_status" {
  api_id             = aws_apigatewayv2_api.http_api.id
  integration_type   = "AWS_PROXY"
  integration_uri    = "arn:aws:lambda:${local.region}:${local.account_id}:function:$${stageVariables.status_function}"
  #integration_uri    = aws_lambda_function.mdf-connect-containerized-status[each.key].invoke_arn
  #integration_uri    = "aws_lambda_function.${local.namespace}-status-${each.key}".invoke_arn
  integration_method = "POST"
  #integration_payload_format_version = "2.0"
  #integration_timeout_ms = 5000
  #route_key          = "POST /submission-status"
}
