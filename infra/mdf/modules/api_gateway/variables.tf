variable "env" {
  type = string
  description = "Either 'dev' or 'prod'"
}

variable "mdf_connect_authorizer_invoke_arn" {
  description = "The invoke ARN of the GardenAuthorizer Lambda function"
  type        = string
}


variable "mdf_connect_authorizer_function_name" {
  description = "The function name of the GardenAuthorizer Lambda function"
  type        = string
}

variable "submit_lambda_invoke_arn" {
  description = "The invoke ARN of the Submit Lambda function"
  type        = string
}

variable "submit_lambda_function_name" {
  description = "The name of the Submit Lambda function"
  type        = string
}

variable "status_lambda_invoke_arn" {
  description = "The invoke ARN of the Status Lambda function"
  type        = string
}

variable "status_lambda_function_name" {
  description = "The name of the Status Lambda function"
  type        = string
}

variable "submissions_lambda_invoke_arn" {
    description = "The invoke ARN of the Submissions Lambda function"
    type        = string
}

variable "submissions_lambda_function_name" {
    description = "The name of the Submissions Lambda function"
    type        = string
}

variable "list_datasets_lambda_invoke_arn" {
    description = "The invoke ARN of the List Datasets Lambda function"
    type        = string
}

variable "list_datasets_lambda_function_name" {
    description = "The name of the List Datasets Lambda function"
    type        = string
}

variable "get_metadata_lambda_invoke_arn" {
    description = "The invoke ARN of the Get Metadata Lambda function"
    type        = string
}

variable "get_metadata_lambda_function_name" {
    description = "The name of the Get Metadata Lambda function"
    type        = string
}

variable "update_metadata_lambda_invoke_arn" {
    description = "The invoke ARN of the Update Metadata Lambda function"
    type        = string
}

variable "update_metadata_lambda_function_name" {
    description = "The name of the Update Metadata Lambda function"
    type        = string
}

variable "get_versions_lambda_invoke_arn" {
    description = "The invoke ARN of the Get Versions Lambda function"
    type        = string
}

variable "get_versions_lambda_function_name" {
    description = "The name of the Get Versions Lambda function"
    type        = string
}



