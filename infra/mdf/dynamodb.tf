resource "aws_dynamodb_table" "dynamodb-table" {
  for_each       = local.environments
  name           = "${local.namespace}-${each.key}"
  billing_mode   = "PROVISIONED"
  read_capacity  = 5
  write_capacity = 5
  hash_key       = "source_id"
  range_key      = "version"
  attribute {
    name = "source_id"
    type = "S"
  }

  attribute {
    name = "version"
    type = "S"
  }

  ttl {
    attribute_name = "TimeToExist"
    enabled        = false
  }

  tags = {
    Name        = "${local.namespace}"
    Environment = "${each.key}"
  }
}
