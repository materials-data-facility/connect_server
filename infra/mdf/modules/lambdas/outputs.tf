output "mdf_connect_authorizer_invoke_arn" {
  value = aws_lambda_function.mdf-connect-auth.invoke_arn
}

output "mdf_connect_authorizer_function_name" {
  value = aws_lambda_function.mdf-connect-auth.function_name
}

output "submit_lambda_invoke_arn" {
    value = aws_lambda_function.mdf-connect-submit.invoke_arn
}

output "submit_lambda_function_name" {
    value = aws_lambda_function.mdf-connect-submit.function_name
}

output "status_lambda_invoke_arn" {
    value = aws_lambda_function.mdf-connect-status.invoke_arn
}

output "status_lambda_function_name" {
    value = aws_lambda_function.mdf-connect-status.function_name
}

output "submissions_lambda_invoke_arn" {
    value = aws_lambda_function.mdf-connect-submissions.invoke_arn
}

output "submissions_lambda_function_name" {
    value = aws_lambda_function.mdf-connect-submissions.function_name
}
