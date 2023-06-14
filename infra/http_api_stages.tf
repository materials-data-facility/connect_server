
# Define the AWS API Gateway v2 deployment for production
resource "aws_apigatewayv2_stage" "prod" {
  name        = "prod"
  api_id      = aws_apigatewayv2_api.http_api.id
  auto_deploy = true
}

# Define the AWS API Gateway v2 deployment for testing
resource "aws_apigatewayv2_stage" "test" {
  name        = "test"
  api_id      = aws_apigatewayv2_api.http_api.id
  auto_deploy = true
}# Output the URLs for each environment
output "auth_url" {
  value = "${aws_apigatewayv2_stage.prod.invoke_url}/auth"
}

output "submit_dataset_url" {
  value = "${aws_apigatewayv2_stage.prod.invoke_url}/submit-dataset"
}

output "submission_status_url" {
  value = "${aws_apigatewayv2_stage.prod.invoke_url}/submission-status/{submission_id}"
}

output "auth_url_test" {
  value = "${aws_apigatewayv2_stage.test.invoke_url}/auth"
}

output "submit_dataset_url_test" {
  value = "${aws_apigatewayv2_stage.test.invoke_url}/submit-dataset"
}

output "submission_status_url_test" {
  value = "${aws_apigatewayv2_stage.test.invoke_url}/submission-status/{submission_id}"
}
