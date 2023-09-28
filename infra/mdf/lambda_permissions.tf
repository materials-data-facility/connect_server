resource "aws_lambda_permission" "lambda_auth_permission" {
  statement_id  = "AllowAPIGatewayInfoke"
  action        = "lambda:InvokeFunction"
  function_name = "MDF-Connect-auth-prod"
  principal     = "apigateway.amazonaws.com"

  # The /* part allows invocation from any stage, method and resource path
  # within API Gateway.
  #source_arn = "${aws_api_gateway_rest_api.MyDemoAPI.execution_arn}/*"
  #source_arn = "${aws_apigatewayv2_api.http_api.execution_arn}/*"
}
resource "aws_lambda_permission" "lambda_submit_permission" {
  statement_id  = "AllowAPIGatewayInfoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.mdf-connect-containerized-submit["prod"].function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_apigatewayv2_api.http_api.execution_arn}/*/*"

  # The /* part allows invocation from any stage, method and resource path
  # within API Gateway.
  #source_arn = "${aws_api_gateway_rest_api.MyDemoAPI.execution_arn}/*"
  #source_arn = "${aws_apigatewayv2_api.http_api.execution_arn}/*"
}
