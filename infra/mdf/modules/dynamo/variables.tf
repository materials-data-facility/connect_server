variable "env" {
  description = "The environment for the deployment. (dev, prod)"
  type        = string
}

variable "namespace" {
  description = "The namespace for the deployment."
  type        = string
}

variable "env_vars" {
  description = "Set of environment variables for the functions."
  type = map(string)
}

variable "resource_tags" {
  description = "Tags to apply to all resources."
  type = map(string)
}

variable "dynamodb_write_capacity" {
  type = number
  description = "The write capacity for the DynamoDB table."
}