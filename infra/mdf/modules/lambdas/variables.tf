variable "env" {
  description = "The environment for the deployment. (dev, prod)"
  type        = string
}

variable "namespace" {
  description = "The namespace for the deployment."
  type        = string
}

variable "lambda_execution_role_arn" {
    type = string
    description = "ARN of the Lambda Execution Role"
}

variable "env_vars" {
  description = "Set of environment variables for the functions."
  type = map(string)
}

variable "ecr_repos" {
  description = "Map of lambda function to ECR repo holding the images."
  type = map(string)
}

variable "resource_tags" {
  description = "Tags to apply to all resources."
  type = map(string)
}
