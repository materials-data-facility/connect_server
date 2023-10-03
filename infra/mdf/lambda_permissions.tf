resource "aws_lambda_permission" "lambda_auth_permission" {
  for_each = local.environments
  statement_id  = "AllowAPIGatewayInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.mdf-connect-containerized-auth["${each.key}"].function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_apigatewayv2_api.http_api[each.key].execution_arn}/*/*"
}

resource "aws_lambda_permission" "lambda_submit_permission" {
  for_each = local.environments
  statement_id  = "AllowAPIGatewayInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.mdf-connect-containerized-submit["${each.key}"].function_name
  #function_name = "${aws_lambda_alias.submit_alias["${each.key}"].function_name}"
  principal     = "apigateway.amazonaws.com"
  #source_arn    = "${aws_apigatewayv2_api.http_api[each.key].execution_arn}/${each.key}/submit"
   #This is the source_arn that the console suggests to add to the permission:
  #source_arn    = "arn:aws:execute-api:us-east-1:557062710055:6oqmi1rtp2/*/*/submit"
  #source_arn    = "${aws_apigatewayv2_api.http_api[each.key].execution_arn}/${each.key}/*/submit"
  source_arn    = "arn:aws:execute-api:us-east-1:557062710055:6oqmi1rtp2/*/*"
  #source_arn    = "${aws_apigatewayv2_api.http_api[each.key].execution_arn}/*/submit"
  #qualifier = "submit-alias-${each.key}"
  #qualifier = "${aws_lambda_alias.submit_alias["${each.key}"].function_version}"

  #qualifier     = aws_lambda_function.mdf-connect-containerized-submit["${each.key}"].function_name.lambda_function_version

}

resource "aws_lambda_permission" "lambda_status_permission" {
  for_each = local.environments
  statement_id  = "AllowAPIGatewayInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.mdf-connect-containerized-status["${each.key}"].function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_apigatewayv2_api.http_api[each.key].execution_arn}/*/*/*"
}
