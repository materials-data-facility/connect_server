
resource "aws_dynamodb_table" "dynamodb-table" {
  name           = "${var.namespace}-${var.env}"
  billing_mode   = "PROVISIONED"
  read_capacity  = var.dynamodb_read_capacity
  write_capacity = var.dynamodb_write_capacity
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

  # Workaround frm https://github.com/hashicorp/terraform-provider-aws/issues/10304#issuecomment-1672617928
  ttl {
    attribute_name = ""
    enabled        = false
  }

  tags = var.resource_tags
}
