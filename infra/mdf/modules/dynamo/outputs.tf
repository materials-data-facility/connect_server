output "dynamodb_arn" {
  value = aws_dynamodb_table.dynamodb-table.arn
}

output "updated_envs" {
  value = merge(var.env_vars,
    { DYNAMO_STATUS_TABLE = aws_dynamodb_table.dynamodb-table.name }
  )
}
