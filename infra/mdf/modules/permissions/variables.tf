variable "env" {
  description = "The environment for the deployment. (dev, prod)"
  type        = string
}

variable "namespace" {
  description = "The namespace for the deployment."
  type        = string
}

variable "mdf_secrets_arn" {
  type = string
  description = "ARN of the MDF Secrets Manager"
}

variable "dynamo_db_arn" {
    type = string
    description = "ARN of the DynamoDB table"
}

variable "legacy_table_arn" {
    type = string
    description = "ARN of the legacy DynamoDB table"
}